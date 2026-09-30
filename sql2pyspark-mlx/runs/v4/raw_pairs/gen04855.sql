SELECT *, quantity - unit_price AS line_total, unit_price - quantity AS line_total_2
FROM order_items
WHERE quantity IS NOT NULL
ORDER BY line_total_2 DESC, product_id, id
LIMIT 3
