WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(country, '/', city) AS country_city,
        LENGTH(city) AS city_length,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        rating,
        city_length,
        city_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY city_length DESC NULLS LAST, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_launch_date
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY DATE_TRUNC('WEEK', product_launch_date)), 4) AS rating_share,
        LEAD(rating, 2) OVER (ORDER BY city_length, id) AS next_rating
    FROM with_latest
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        rating_share,
        next_rating,
        LEAD(next_rating, 1) OVER w_part AS next_next_rating,
        LAG(rating_share, 1) OVER (ORDER BY rating, id) AS prev_rating_share,
        AVG(rating) OVER w_recent AS rating_avg_6
    FROM with_windows
    WHERE rating <= 3.9
    WINDOW w_part AS (ORDER BY rating_share, id), w_recent AS (ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_latest_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_stock_qty
    FROM enriched s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_rating_share >= 0.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_rating_share_tier,
    CASE WHEN product_stock_qty >= 362 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_stock_qty_tier,
    ROUND(AVG(product_stock_qty), 2) AS avg_product_stock_qty,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows
FROM with_latest_suppliers
GROUP BY 1, 2;
