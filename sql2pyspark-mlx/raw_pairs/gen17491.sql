SELECT QUARTER(s.opened_date) AS opened_quarter, MAX(o.discount) - MIN(o.discount) AS discount_range
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY QUARTER(s.opened_date)
HAVING MAX(o.discount) - MIN(o.discount) >= 74
ORDER BY discount_range DESC, opened_quarter DESC
LIMIT 5
