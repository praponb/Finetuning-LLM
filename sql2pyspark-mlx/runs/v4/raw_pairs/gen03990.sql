SELECT s.*, latest.unit_price
FROM orders s
JOIN LATERAL (
        SELECT c.unit_price
        FROM order_items c
        WHERE c.order_id = s.id
        ORDER BY c.quantity DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
