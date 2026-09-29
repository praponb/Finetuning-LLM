SELECT YEAR(o.order_date) order_year, COUNT(DISTINCT oi.quantity) unique_orders, COUNT(*) row_count
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN orders o ON o.id = oi.order_id
GROUP BY YEAR(o.order_date)
ORDER BY order_year DESC;
