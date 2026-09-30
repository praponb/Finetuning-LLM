WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        CONCAT(channel, ', ', status) AS channel_status,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM orders
    WHERE shipping_fee > 27
),
eligible AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at >= s.order_date
        )
),
deduped AS (
    SELECT
        id,
        customer_id,
        discount,
        channel_status,
        resolved_channel
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS dedup_rank
        FROM eligible
    ) t
    WHERE dedup_rank = 1
),
kept_keys AS (
    SELECT customer_id
    FROM deduped
    EXCEPT DISTINCT
    SELECT customer_id
    FROM orders
    WHERE shipping_fee IS NULL
),
kept_rows AS (
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.customer_id = k.customer_id
),
summary AS (
    SELECT
        customer_id,
        CASE WHEN discount >= 0.31 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
        STDDEV_SAMP(discount) AS discount_stddev,
        APPROX_PERCENTILE(discount, 0.5) AS discount_p50
    FROM kept_rows
    GROUP BY GROUPING SETS ((customer_id, CASE WHEN discount >= 0.31 THEN 'PREMIUM' ELSE 'STANDARD' END), (customer_id), ())
)
SELECT customer_id
FROM summary
ORDER BY customer_id DESC NULLS LAST;
