WITH with_latest AS (
    SELECT s.*, latest.payment_amount
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    payment_amount
FROM with_latest
ORDER BY amount DESC NULLS LAST
