WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        CASE WHEN discount >= 0.43 THEN 'High' WHEN discount >= 0.1 THEN 'Medium' ELSE 'Low' END AS discount_band,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-11-03'
      AND channel IS NOT NULL
      AND order_date < '2025-08-01'
),
scored AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount_band,
        resolved_channel,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY amount ASC, id ASC) AS amount_bucket,
        AVG(amount) OVER (PARTITION BY customer_id, channel ORDER BY order_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS amount_avg_6,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY amount ASC) AS amount_percent_rank,
        LEAD(amount, 2, 0) OVER (PARTITION BY channel ORDER BY order_date ASC, id ASC) AS next_amount
    FROM cleaned
),
kept AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IN (26, 34)
        )
),
filtered AS (
    SELECT *, LEAST(amount_avg_6, amount_percent_rank) AS least_amount_avg_6_amount_percent_rank
    FROM kept
    WHERE amount_avg_6 <= 1319
),
base AS (
    SELECT
        *,
        UPPER(channel) AS channel_upper,
        CASE WHEN channel IN ('partner', 'store') THEN 'Priority' ELSE 'Other' END AS channel_group
    FROM filtered
    WHERE channel IS NOT NULL
)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        least_amount_avg_6_amount_percent_rank,
        amount
    FROM base
) src
UNPIVOT (
    metric_value FOR kpi IN (least_amount_avg_6_amount_percent_rank, amount AS AMOUNT)
)
