WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS line
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        qty,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY qty DESC) AS qty_percent_rank,
        AVG(qty) OVER (PARTITION BY customer_id) AS customer_id_avg_qty
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN qty_percent_rank >= 0.76 THEN 'High' WHEN qty_percent_rank >= 0.37 THEN 'Medium' ELSE 'Low' END AS qty_percent_rank_band
    FROM enriched
    WHERE customer_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list,
    COUNT(*) FILTER (WHERE code = 'VIP') AS vip_rows
FROM filtered
GROUP BY 1
ORDER BY vip_rows DESC;
