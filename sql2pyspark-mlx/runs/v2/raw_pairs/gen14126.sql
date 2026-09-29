SELECT
    CASE WHEN age >= 61 THEN 'high' WHEN age >= 34 THEN 'mid' ELSE 'low' END age_bucket,
    DATE_TRUNC('MONTH', signup_date) signup_month,
    ROUND((100 * SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_smb,
    MIN(age) min_age
FROM customers
GROUP BY CASE WHEN age >= 61 THEN 'high' WHEN age >= 34 THEN 'mid' ELSE 'low' END, DATE_TRUNC('MONTH', signup_date);
