SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, SUM(CASE WHEN s.device = 'tablet' THEN 1 ELSE 0 END) AS tablet_count
FROM sessions s
LEFT OUTER JOIN customers c ON c.id = s.customer_id
WHERE c.email NOT LIKE '%@company.co'
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM')
ORDER BY tablet_count DESC, signup_month
LIMIT 3
