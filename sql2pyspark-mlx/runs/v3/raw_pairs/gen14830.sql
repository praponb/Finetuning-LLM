SELECT c.city, CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, COUNT(CASE WHEN p.method = 'card' THEN 1 END) AS card_count
FROM payments p
RIGHT OUTER JOIN orders o ON p.order_id = o.id
JOIN customers c ON o.customer_id = c.id
WHERE o.channel IN ('store', 'partner', 'mobile')
GROUP BY c.city, CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY signup_cohort;
