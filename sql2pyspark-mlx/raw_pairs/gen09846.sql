WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), order_date) / 7 AS weeks_since_order_date
    FROM orders
    WHERE store_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_date,
        channel,
        amount,
        shipping_fee,
        SUM(amount) OVER (PARTITION BY store_id ORDER BY weeks_since_order_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_amount,
        MAX(amount) OVER (PARTITION BY store_id ORDER BY weeks_since_order_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_amount,
        COUNT(amount) OVER (PARTITION BY store_id ORDER BY weeks_since_order_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_amount,
        AVG(amount) OVER (PARTITION BY store_id ORDER BY weeks_since_order_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_amount,
        LAST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY status, channel ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee
    FROM base
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date
        )
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL '14' DAY
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.order_date,
        s.amount,
        s.last4_avg_amount,
        s.last_known_shipping_fee,
        s.promo_id,
        s.promo_code,
        s.discount_pct,
        latest.unit_price
    FROM matched s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN unit_price >= 380 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    PERCENTILE_APPROX(last4_avg_amount, ARRAY(0.25, 0.5, 0.75)) AS last4_avg_amount_quartiles,
    ROUND(AVG(last_known_shipping_fee), 2) AS avg_last_known_shipping_fee
FROM with_latest
GROUP BY CASE WHEN unit_price >= 380 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', order_date), DATE_TRUNC('QUARTER', order_date)
