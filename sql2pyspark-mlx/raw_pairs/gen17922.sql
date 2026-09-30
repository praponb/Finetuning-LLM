SELECT CASE WHEN s.country IN ('DE', 'JP') THEN 'group_a' ELSE 'other' END AS country_group, s.city, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand IS NULL
GROUP BY CASE WHEN s.country IN ('DE', 'JP') THEN 'group_a' ELSE 'other' END, s.city
HAVING ROUND(STDDEV(oi.unit_price), 2) > 30
ORDER BY country_group, city DESC;
