SELECT
    segment,
    AVG(age) avg_age,
    SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) smb_count,
    SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) bangkok_count
FROM customers
WHERE email LIKE '%@startup.io' AND MONTH(signup_date) IN (5, 7, 8)
GROUP BY segment
HAVING SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) >= 80
ORDER BY smb_count DESC;
