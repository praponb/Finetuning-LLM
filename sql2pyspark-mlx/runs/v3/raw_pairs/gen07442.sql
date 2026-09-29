SELECT YEAR(o.order_date) AS order_year, COUNT(oi.quantity) AS quantity_count
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
WHERE UPPER(c.name) LIKE '%IM %'
GROUP BY YEAR(o.order_date)
ORDER BY order_year
