WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        pos + 1 AS line_no,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts t
    LATERAL VIEW POSEXPLODE(t.items) li AS pos, li_item
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        line_no,
        qty,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, AVG(line_no) OVER (PARTITION BY code) AS code_avg_line_no
    FROM lines
    WHERE code = 'BUNDLE'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(qty * 0.9, 2) AS qty_scaled
    FROM enriched
    WHERE code IS DISTINCT FROM 'SPRING10'
      AND line_no = 2
)
-- Multi-level aggregation
SELECT
    code,
    COUNT(DISTINCT code) AS distinct_codes,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(code)) AS num_codes,
    ROUND(SUM(code_avg_line_no) / NULLIF(COUNT(CASE WHEN code = 'BUNDLE' THEN 1 END), 0), 2) AS code_avg_line_no_per_bundle,
    COUNT(*) AS cnt_2
FROM valid
GROUP BY code
