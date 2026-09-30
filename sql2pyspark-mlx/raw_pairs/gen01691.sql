SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.amount,
    s.shipping_fee,
    x.payment_method
FROM orders s
LEFT JOIN LATERAL (
        SELECT c.method AS payment_method
        FROM payments c
        WHERE c.order_id = s.id
        ORDER BY c.amount DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
