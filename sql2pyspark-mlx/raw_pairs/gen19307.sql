WITH with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_channel,
        top1.order_shipping_fee
    FROM customers s
    JOIN LATERAL (
            SELECT c.channel AS order_channel, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    name,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    order_channel
FROM with_last;
