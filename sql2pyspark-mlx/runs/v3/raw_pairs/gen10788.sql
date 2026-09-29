SELECT
    o.id,
    o.amount,
    o.customer_id,
    t.max_unit_price
FROM orders o
JOIN (SELECT order_id, MAX(unit_price) AS max_unit_price
    FROM order_items
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.max_unit_price >= 110;
