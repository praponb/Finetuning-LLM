WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_stock_qty
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city) AS city_sum_rating,
        NTILE(4) OVER (PARTITION BY country, city ORDER BY rating DESC, id) AS rating_bucket,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
    FROM with_last
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost <= 610
        )
),
with_last_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.brand
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        product_stock_qty,
        city_sum_rating,
        rating_bucket,
        rating_share,
        brand,
        LAG(rating_share) OVER w_running AS prev_rating_share,
        LEAD(rating_share) OVER w_running AS next_rating_share,
        LEAD(product_stock_qty) OVER w_running AS next_product_stock_qty,
        LAG(product_stock_qty) OVER w_running AS prev_product_stock_qty,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM with_last_suppliers
    WINDOW w_running AS (PARTITION BY city, brand ORDER BY city_sum_rating, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_rating_share >= 0.85 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_rating_share_tier,
    country,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count,
    PERCENTILE_APPROX(product_stock_qty, ARRAY(0.25, 0.5, 0.75)) AS product_stock_qty_quartiles,
    APPROX_PERCENTILE(rating_bucket, 0.5) AS rating_bucket_p50
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN prev_rating_share >= 0.85 THEN 'PREMIUM' ELSE 'STANDARD' END, country), (CASE WHEN prev_rating_share >= 0.85 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY globex_count NULLS FIRST
