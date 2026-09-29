SELECT is_active, smb_credit_limit, age_count
FROM (
    SELECT is_active, SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) AS smb_credit_limit, COUNT(age) AS age_count
    FROM customers
    GROUP BY is_active
) AS sub
WHERE smb_credit_limit < 52
ORDER BY age_count;
