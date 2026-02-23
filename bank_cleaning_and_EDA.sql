SELECT *
FROM bank_dataset;

-- MISSION:
-- 1. Remove Duplicate
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns



-- 1. REMOVE DUPLICATE
CREATE TABLE bu1 LIKE bank_dataset;

INSERT bu1
SELECT * FROM bank_dataset;

SELECT *
FROM bu1;

SELECT *,
	ROW_NUMBER() OVER(PARTITION BY age, job, marital, education, `default`, balance, housing, loan, contact, `day`, `month`, duration, campaign, pdays, previous, poutcome, y) AS row_num
FROM bu1
ORDER BY row_num DESC;

WITH duplicate_cte AS
	(
    SELECT *,
	ROW_NUMBER() OVER(PARTITION BY age, job, marital, education, `default`, balance, housing, loan, contact, `day`, `month`, duration, campaign, pdays, previous, poutcome, y) AS row_num
	FROM bu1
    )
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `bu2` (
  `age` int DEFAULT NULL,
  `job` text,
  `marital` text,
  `education` text,
  `default` text,
  `balance` int DEFAULT NULL,
  `housing` text,
  `loan` text,
  `contact` text,
  `day` int DEFAULT NULL,
  `month` text,
  `duration` int DEFAULT NULL,
  `campaign` int DEFAULT NULL,
  `pdays` int DEFAULT NULL,
  `previous` int DEFAULT NULL,
  `poutcome` text,
  `y` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT* FROM bu2;

INSERT INTO bu2
SELECT*,
	ROW_NUMBER() OVER(PARTITION BY age, job, marital, education, `default`,
    balance, housing, loan, contact, `day`, `month`,
    duration, campaign, pdays, previous, poutcome, y) AS row_num
FROM bu1;
	
DELETE
FROM bu2
WHERE row_num > 1;

SELECT*
FROM bu2
WHERE row_num > 1;

SELECT *
FROM bu2;


-- 2. STANDARDIZING DATA

SELECT *
FROM bu2;

SELECT DISTINCT(job)
FROM bu2;

SELECT TRIM(age)
FROM bu2;

UPDATE bu2
SET 
age = TRIM(age), job = TRIM(job), marital = TRIM(marital), education = TRIM(education), `default` = TRIM(`default`), balance = TRIM(balance),
housing = TRIM(housing), loan = TRIM(loan),
contact = TRIM(contact), `day` = TRIM(`day`), `month` = TRIM(`month`), duration = TRIM(duration),
campaign = TRIM(campaign), pdays = TRIM(pdays), previous = TRIM(previous), poutcome = TRIM(poutcome), y = TRIM(y);


SELECT DISTINCT job
FROM bu2
ORDER BY job;

UPDATE bu2
SET job = LOWER(job);

UPDATE bu2
SET job = 'administrative' WHERE job = 'admin.';


SELECT DISTINCT marital
FROM bu2;
UPDATE bu2
SET marital = 'divorced' WHERE marital = 'div.';
UPDATE bu2
SET marital = LOWER(marital);

SELECT DISTINCT education
FROM bu2;
UPDATE bu2
SET education = 'unknown' WHERE education = 'UNK';
UPDATE bu2
SET education='secondary' WHERE education = 'sec.';
UPDATE bu2
SET education = LOWER(education);

SELECT COUNT(*)
FROM bu2;

SELECT DISTINCT contact
FROM bu2;
UPDATE bu2
SET contact='cellular' WHERE contact = 'mobile';
UPDATE bu2
SET contact='telephone' WHERE contact = 'phone';
UPDATE bu2
SET contact = LOWER(contact);

SELECT DISTINCT poutcome
FROM bu2
ORDER BY poutcome;
UPDATE bu2
SET poutcome = LOWER(poutcome);
UPDATE bu2
SET poutcome='unknown' WHERE poutcome = 'unk';

UPDATE bu2
SET age = NULL WHERE age > 100;

SELECT DISTINCT age
FROM bu2
ORDER BY age;

SELECT*
FROM bu2
LIMIT 100;



-- 3. NULL VALUES AND BLANK VALUES

SELECT 
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS Umur_Null,          -- Wajib DIbenerin
    SUM(CASE WHEN age = 0 THEN 1 ELSE 0 END) AS Umur_Nol,               -- Wajib dibenerin (Bayi?)
    SUM(CASE WHEN age IS NULL OR age = 0 THEN 1 ELSE 0 END) AS Total_Umur_Rusak, -- Target Operasi
    SUM(CASE WHEN job = 'unknown' THEN 1 ELSE 0 END) AS Job_Unknown,    -- Label bawaan (Aman)
    SUM(CASE WHEN job = '' THEN 1 ELSE 0 END) AS Job_Blank,             -- Harus diubah jadi 'unknown'
    SUM(CASE WHEN job IS NULL THEN 1 ELSE 0 END) AS Job_Null,           -- Harus diubah jadi 'unknown'
    SUM(CASE WHEN balance IS NULL THEN 1 ELSE 0 END) AS Balance_Null,   -- WAJIB DIBENERIN (Target Operasi)
    SUM(CASE WHEN balance = 0 THEN 1 ELSE 0 END) AS Balance_Nol,        -- Info doang (Valid)
    SUM(CASE WHEN balance < 0 THEN 1 ELSE 0 END) AS Balance_Minus       -- Info doang (Valid/Ngutang)
FROM bu2;

-- 1. SIAPIN DULU VARIBEL RATA-RATANYA (Biar gak error 1093)
-- hitung rata-rata Age (yg valid) dan Balance (yg valid)
SET @rata_umur = (SELECT AVG(age) FROM bu2 WHERE age IS NOT NULL AND age > 0);
SET @rata_saldo = (SELECT AVG(balance) FROM bu2 WHERE balance IS NOT NULL);

-- 2. EKSEKUSI UPDATE MASSAL (Sapu Jagat)
UPDATE bu2
SET -- Update AGE: Kalau NULL atau 0, ganti pake @rata_umur. Sisanya TETAP.
    age = CASE 
            WHEN age IS NULL OR age = 0 THEN @rata_umur 
            ELSE age 
          END,
	-- Update JOB: Kalau NULL atau Kosong, ganti 'unknown'. Sisanya TETAP.
    job = CASE 
            WHEN job IS NULL OR job = '' THEN 'unknown' 
            ELSE job 
          END,
	-- Update BALANCE: Kalau NULL, ganti pake @rata_saldo. Sisanya TETAP.
    -- (Inget: 0 dan Minus dibiarin karena masuk ke ELSE)
    balance = CASE 
                WHEN balance IS NULL THEN @rata_saldo 
                ELSE balance 
            END,
	-- Sekalian kolom lain (Bonus biar rapi)
    education = CASE 
                  WHEN education IS NULL OR education = '' THEN 'unknown' 
                  ELSE education 
                END,
    contact = CASE 
                WHEN contact IS NULL OR contact = '' THEN 'unknown' 
                ELSE contact 
              END;
	
    
    SELECT*
    FROM bu2
    LIMIT 100;
    
SELECT 
    '1. TOTAL BARIS (Cek Duplikat)' AS Indikator, 
    COUNT(*) AS Hasil_Angka, 
    'Harus sesuai sama jumlah data bersih (sekitar 45.207)' AS Target
FROM bu2
UNION ALL
SELECT 
    '2. SISA NULL DI AGE', 
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END), 
    'HARUS 0 (NOL)!'
FROM bu2
UNION ALL
SELECT 
    '3. SISA NULL DI BALANCE', 
    SUM(CASE WHEN balance IS NULL THEN 1 ELSE 0 END), 
    'HARUS 0 (NOL)!'
FROM bu2
UNION ALL
SELECT 
    '4. MAX UMUR (LOGIKA)', 
    MAX(age), 
    'Harus di bawah 100 (Masuk Akal)'
FROM bu2
UNION ALL
SELECT 
    '5. MIN UMUR (LOGIKA)', 
    MIN(age), 
    'Harus di atas 17 (Dewasa)'
FROM bu2;

SELECT DISTINCT job FROM bu2 ORDER BY job;
SELECT DISTINCT marital FROM bu2 ORDER BY marital;
SELECT DISTINCT education FROM bu2 ORDER BY education;
SELECT DISTINCT `month` FROM bu2 ORDER BY `month`;

UPDATE bu2
SET marital = 'unknown'
WHERE marital = '' OR marital = ' ' OR marital IS NULL;

ALTER TABLE bu2 DROP COLUMN row_num;

-- 1. Ganti Target Variable (Paling Penting)
ALTER TABLE bu2 CHANGE COLUMN y deposit_subscribed TEXT;

-- 2. Perjelas Jenis Loan
ALTER TABLE bu2 CHANGE COLUMN housing housing_loan TEXT;
ALTER TABLE bu2 CHANGE COLUMN loan personal_loan TEXT;

-- 3. Perjelas Kolom Angka/Hitungan
ALTER TABLE bu2 CHANGE COLUMN campaign campaign_count INT;
ALTER TABLE bu2 CHANGE COLUMN previous previous_contact_count INT;
ALTER TABLE bu2 CHANGE COLUMN duration call_duration_sec INT;

-- 4. Perjelas Konteks Waktu & Hasil
ALTER TABLE bu2 CHANGE COLUMN poutcome prev_campaign_outcome TEXT;
ALTER TABLE bu2 CHANGE COLUMN day contact_day INT;
ALTER TABLE bu2 CHANGE COLUMN month contact_month TEXT;

SELECT*
FROM bu2;



-- EDA STEPS


SELECT
	job,
	SUM(CASE WHEN deposit_subscribed = 'yes' THEN 1 ELSE 0 END) AS jumlah_yes,
    COUNT(*) AS jumlah_orang,
    (SUM(CASE WHEN deposit_subscribed = 'yes' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS persentase_sukses
FROM bu2
GROUP BY job
ORDER BY persentase_sukses DESC;

SELECT
	contact_month,
	SUM(CASE WHEN deposit_subscribed = 'yes' THEN 1 ELSE 0 END) AS jumlah_kejadian_yes
FROM bu2
GROUP BY contact_month
ORDER BY jumlah_kejadian_yes DESC;

SELECT
	deposit_subscribed AS status_deposit,
	COUNT(*) AS jumlah_orang,
    AVG(balance) AS rata_rata_saldo
FROM bu2
GROUP BY deposit_subscribed;



	