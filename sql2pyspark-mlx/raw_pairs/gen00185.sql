WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) AS amount_share,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_avg_30d,
        LAG(amount, 1) OVER (PARTITION BY TO_DATE(paid_at), status ORDER BY paid_at, id) AS prev_amount
    FROM payments
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.amount_avg_30d,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS sum_order_item_quantity
FROM enriched s
ORDER BY method NULLS FIRST
