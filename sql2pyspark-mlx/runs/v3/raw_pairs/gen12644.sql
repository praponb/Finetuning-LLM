SELECT YEAR(s.opened_date) AS opened_year, MAX(o.discount) - MIN(o.discount) AS discount_range, COUNT(CASE WHEN o.channel = 'partner' THEN 1 END) AS partner_count
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY YEAR(s.opened_date)
ORDER BY opened_year DESC
LIMIT 10
