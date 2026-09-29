SELECT is_active, ROUND((100 * SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_smb
FROM customers
WHERE country = 'TH'
GROUP BY is_active
ORDER BY pct_smb DESC
