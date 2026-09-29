SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, COUNT(CASE WHEN o.channel = 'partner' THEN 1 END) AS partner_count, COUNT(*) AS cnt
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING partner_count >= 2;
