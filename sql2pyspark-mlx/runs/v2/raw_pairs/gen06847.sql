SELECT oi.unit_price, o.order_date, oi.unit_price * oi.quantity AS line_total
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.amount < 1550 OR oi.quantity <> 5
ORDER BY line_total DESC, unit_price DESC;
