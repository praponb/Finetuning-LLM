WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(city) AS city_length,
        COALESCE(city, 'N/A') AS city_filled,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE rating <= 2.5
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category NOT IN ('Home', 'Fashion', 'Sports')
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_price
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country ORDER BY product_price NULLS FIRST) AS product_price_cume_dist,
        AVG(city_length) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS city_length_avg_6_prior
    FROM with_last
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY city_length DESC NULLS FIRST, id) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        city_length,
        city_clean,
        product_price,
        product_price_cume_dist,
        city_length_avg_6_prior,
        row_num,
        AVG(product_price_cume_dist) OVER (PARTITION BY country ORDER BY product_price_cume_dist, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS product_price_cume_dist_avg_2_prior,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM ranked
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        city_length,
        city_clean,
        product_price,
        product_price_cume_dist,
        city_length_avg_6_prior,
        row_num,
        product_price_cume_dist_avg_2_prior,
        rating_share,
        COALESCE(product_price_cume_dist, 0) AS product_price_cume_dist_filled
    FROM enriched
    WHERE city_length BETWEEN 35 AND 52
)
SELECT
    id,
    city,
    city_clean,
    product_price,
    product_price_cume_dist,
    city_length_avg_6_prior,
    product_price_cume_dist_avg_2_prior,
    rating_share,
    product_price_cume_dist_filled,
    CASE WHEN city RLIKE '^(Singapore|Tokyo)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS city_zone
FROM valid
