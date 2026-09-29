SELECT MONTH(signup_date) AS signup_month, segment, city, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count FROM customers GROUP BY 1, 2, 3
