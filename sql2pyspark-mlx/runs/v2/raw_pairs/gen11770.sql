SELECT QUARTER(s.opened_date) AS opened_quarter, SUM(o.discount) AS total_discount, ROUND((100 * SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile
FROM orders o
JOIN stores s ON o.store_id = s.id
JOIN customers c ON o.customer_id = c.id
GROUP BY QUARTER(s.opened_date)
HAVING SUM(o.discount) > 1.4
