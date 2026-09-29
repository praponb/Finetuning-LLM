WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        rating,
        row_num,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        AVG(rating) OVER w_ordered AS country_avg_rating,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
    FROM deduped
    WHERE country = 'SG'
    WINDOW w_ordered AS (PARTITION BY country)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY country_avg_rating) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_launch_date
    FROM ranked s
    JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_category,
        latest.product_price
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        rating_share,
        country_avg_rating,
        dedup_rank,
        product_launch_date,
        product_price,
        AVG(country_avg_rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS country_avg_rating_avg_2_prior,
        LAST_VALUE(product_category) IGNORE NULLS OVER (ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_category,
        AVG(rating_share) OVER (PARTITION BY YEAR(product_launch_date), city ORDER BY country_avg_rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_share_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY country, product_category ORDER BY country_avg_rating DESC) AS country_avg_rating_dense_rank,
        SUM(country_avg_rating) OVER (PARTITION BY DATE_TRUNC('week', product_launch_date), product_category ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_country_avg_rating
    FROM with_latest
)
-- Ranking and gap features
SELECT
    id,
    rating,
    product_launch_date,
    product_price,
    country_avg_rating_avg_2_prior,
    rating_share_avg_2_prior,
    country_avg_rating_dense_rank,
    cumulative_country_avg_rating,
    LAG(rating, 1, 0) OVER w_recent AS prev_rating,
    NTILE(3) OVER w_recent AS country_avg_rating_avg_2_prior_bucket
FROM scored
WINDOW w_recent AS (PARTITION BY city ORDER BY country_avg_rating_avg_2_prior, id)
