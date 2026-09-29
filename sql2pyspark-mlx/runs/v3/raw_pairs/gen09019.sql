SELECT segment, COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count
FROM customers
WHERE is_active
GROUP BY 1
