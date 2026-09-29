SELECT MONTH(c.signup_date) signup_month, SUM(CASE WHEN s.device = 'mobile' THEN 1 ELSE 0 END) mobile_count, COUNT(CASE WHEN s.country = 'TH' THEN 1 END) th_count
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.device = 'tablet'
GROUP BY MONTH(c.signup_date)
ORDER BY 3 DESC;
