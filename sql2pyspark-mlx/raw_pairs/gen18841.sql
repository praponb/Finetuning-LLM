WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) rating_share,
        NTILE(4) OVER (PARTITION BY country, city ORDER BY rating, id) rating_bucket
    FROM suppliers
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.launch_date
    FROM scored s
    JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.brand
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= s.launch_date
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        country,
        city,
        rating,
        rating_bucket,
        launch_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand, city ORDER BY rating NULLS LAST) AS rank_in_group
        FROM with_last_suppliers
    ) ranked
    WHERE rank_in_group = 1
),
with_last_suppliers_2 AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_brand
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= s.launch_date
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        rating_bucket,
        launch_date,
        product_brand,
        AVG(rating) OVER (ORDER BY rating_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) rating_avg_3_prior,
        SUM(rating) OVER w_part cumulative_rating
    FROM with_last_suppliers_2
    WHERE product_brand NOT IN ('Globex', 'Umbrella', 'Acme')
    WINDOW w_part AS (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Window metrics over several different partitions
SELECT
    id,
    rating_bucket,
    rating_avg_3_prior,
    cumulative_rating,
    MIN(cumulative_rating) OVER w_part running_min_cumulative_rating,
    SUM(cumulative_rating) OVER w_part running_sum_cumulative_rating,
    AVG(cumulative_rating) OVER w_part running_avg_cumulative_rating,
    COUNT(cumulative_rating) OVER w_part running_count_cumulative_rating,
    AVG(rating_bucket) OVER (PARTITION BY product_brand ORDER BY rating_avg_3_prior, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) rating_bucket_avg_5
FROM enriched
WINDOW w_part AS (PARTITION BY city ORDER BY rating_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
