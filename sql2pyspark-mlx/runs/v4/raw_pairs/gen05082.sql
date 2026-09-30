WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id IS NULL
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        LAST_VALUE(amount) IGNORE NULLS OVER w_hist AS last_known_amount,
        NTILE(10) OVER w_recent AS amount_bucket
    FROM eligible
    WINDOW w_hist AS (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY amount, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date >= s.order_date
        )
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            status,
            last_known_amount
        FROM not_flagged
    ) src
    PIVOT (
        MAX(last_known_amount) AS value_sum,
        COUNT(1) AS n
        FOR status IN ('SHIPPED', 'COMPLETED', 'RETURNED')
    )
)
SELECT
    s.customer_id,
    s.SHIPPED_value_sum,
    s.COMPLETED_value_sum,
    s.RETURNED_value_sum,
    s.RETURNED_n,
    (SELECT SUM(x.shipping_fee) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_shipping_fee
FROM wide s
ORDER BY RETURNED_value_sum NULLS LAST, SHIPPED_value_sum
