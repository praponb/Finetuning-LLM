WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        ROUND(rating * 0.85, 2) AS rating_adjusted,
        CONCAT(city, '/', country) AS city_country
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        city,
        rating,
        rating_adjusted,
        city_country,
        AVG(rating_adjusted) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_rating_adjusted,
        MAX(rating_adjusted) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating_adjusted,
        MIN(rating_adjusted) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_rating_adjusted,
        NTILE(10) OVER (PARTITION BY city, country ORDER BY rating_adjusted DESC, id) AS rating_adjusted_bucket
    FROM base
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(rating * 1.15, 2) AS rating_adjusted_2
    FROM enriched
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.rating,
        s.rating_adjusted,
        s.running_max_rating_adjusted,
        s.running_min_rating_adjusted,
        s.rating_adjusted_bucket,
        s.rating_adjusted_2,
        x.brand,
        x.price
    FROM filtered s
    JOIN LATERAL (
            SELECT c.brand, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Ranking and gap features
SELECT
    *,
    LAG(price, 2) OVER (ORDER BY rating_adjusted, id) AS prev_price,
    ROW_NUMBER() OVER (ORDER BY rating_adjusted_2, id) AS row_seq,
    LAG(running_max_rating_adjusted, 2) OVER (ORDER BY rating, id) AS prev_running_max_rating_adjusted,
    CUME_DIST() OVER (PARTITION BY city, brand ORDER BY price DESC NULLS FIRST) AS price_cume_dist,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating_adjusted_2 NULLS FIRST) AS rating_adjusted_2_dense_rank
FROM with_latest;
