SELECT
    DATE_TRUNC('MONTH', signup_date) signup_month,
    segment,
    MAX(age) max_age,
    SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) smb_credit_limit,
    MIN(credit_limit) min_credit_limit
FROM customers
WHERE LOWER(TRIM(name)) = 'olivia garcia' AND name LIKE '%For%'
GROUP BY signup_month, segment
HAVING SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) < 55
ORDER BY min_credit_limit DESC, max_age DESC
