SELECT s.country, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE MONTH(p.launch_date) IN (6, 9, 11)
GROUP BY s.country
ORDER BY stddev_unit_price ASC, country ASC
LIMIT 50
