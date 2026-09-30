WITH base AS (
    SELECT
        id,
        store_id,
        amount,
        discount,
        shipping_fee,
        CONCAT(channel, '/', status) AS channel_status,
        DATE_TRUNC('week', order_date) AS order_date_period,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM orders
    WHERE discount > 0.18
      AND customer_id IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.delivered_date <= s.order_date_period
        )
),
valid AS (
    SELECT
        id,
        store_id,
        discount,
        shipping_fee,
        channel_status,
        channel_filled,
        ROUND(discount * 0.85, 2) AS adjusted_discount,
        CASE WHEN discount >= 0.39 THEN 'High' WHEN discount >= 0.14 THEN 'Medium' ELSE 'Low' END AS discount_tier
    FROM eligible
    WHERE discount <= 0.4
),
latest AS (
    SELECT
        id,
        store_id,
        shipping_fee,
        channel_status,
        channel_filled,
        adjusted_discount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee NULLS FIRST, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY adjusted_discount DESC NULLS FIRST, id) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
)
SELECT
    s.city,
    COUNT(*) AS cnt,
    ROUND(AVG(a.shipping_fee), 2) AS avg_shipping_fee
FROM deduped a
JOIN stores s ON a.store_id = s.id
GROUP BY s.city
ORDER BY avg_shipping_fee
