SELECT s.*, b.discount
FROM order_items s
LEFT JOIN orders b
    ON s.order_id = b.id
