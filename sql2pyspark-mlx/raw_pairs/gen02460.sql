SELECT
    s.id,
    s.city,
    s.segment,
    s.is_active,
    s.credit_limit,
    s.age,
    latest.order_shipping_fee,
    latest.order_discount
FROM customers s
JOIN LATERAL (
        SELECT c.shipping_fee AS order_shipping_fee, c.discount AS order_discount
        FROM orders c
        WHERE c.customer_id = s.id
        ORDER BY c.discount DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
