SELECT country, segment, MAX(signup_date) AS latest_signup_date, SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) AS smb_count FROM customers GROUP BY CUBE(country, segment) ORDER BY segment;
