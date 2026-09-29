SELECT is_active, SUM(credit_limit) AS total_credit_limit, SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) AS smb_count
FROM customers
GROUP BY 1
HAVING SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) < 29;
