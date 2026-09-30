SELECT city, DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) AS smb_count
FROM customers
WHERE (email LIKE '%@gmail.com' AND segment IS NULL) OR email IS NOT NULL
GROUP BY 1, 2;
