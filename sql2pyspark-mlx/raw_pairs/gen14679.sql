WITH valid AS (
    SELECT
        *,
        CONCAT_WS('-', status, channel) AS status_channel_key,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM orders
    WHERE channel IS DISTINCT FROM 'mobile'
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date
        )
),
with_windows AS (
    SELECT
        id,
        order_date,
        channel,
        shipping_fee,
        resolved_channel,
        ROW_NUMBER() OVER (PARTITION BY MONTH(order_date), status ORDER BY amount ASC, id ASC) AS row_seq,
        LAG(discount, 1, 0) OVER (ORDER BY order_date ASC, id ASC) AS prev_discount
    FROM not_flagged
    WHERE discount < 0.27
),
filtered AS (
    SELECT
        id,
        order_date,
        shipping_fee,
        resolved_channel,
        row_seq,
        prev_discount,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        COALESCE(channel, 'N/A') AS resolved_channel_2
    FROM with_windows
    WHERE order_date >= DATE '2023-05-09'
)
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    CASE WHEN shipping_fee >= 45 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_tier,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    APPROX_PERCENTILE(prev_discount, ARRAY(0.25, 0.5, 0.75)) AS prev_discount_quartiles,
    MIN(days_since_order_date) AS min_days_since_order_date,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee_2,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM filtered
GROUP BY week_start, shipping_fee_tier
HAVING min_days_since_order_date > 0
ORDER BY shipping_fee_tier ASC NULLS FIRST;
