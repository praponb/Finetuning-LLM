WITH valid AS (
    SELECT
        id,
        country,
        rating,
        COALESCE(supplier_name, 'N/A') AS supplier_name_filled,
        LOWER(city) AS city_lower,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS NOT DISTINCT FROM 'London'
),
metrics AS (
    SELECT
        id,
        supplier_name_filled,
        city_lower,
        city_clean,
        RANK() OVER (ORDER BY rating NULLS FIRST) AS rating_rank,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM valid
),
with_last AS (
    SELECT
        s.id,
        s.city_clean,
        s.rating_rank,
        s.rating_avg_2_prior,
        s.cumulative_rating,
        x.product_price,
        x.cost
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.price AS product_price, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(rating_avg_2_prior) IGNORE NULLS OVER (ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_avg_2_prior,
        DENSE_RANK() OVER (ORDER BY rating_rank DESC NULLS FIRST) AS rating_rank_dense_rank,
        SUM(cumulative_rating) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_rating,
        DENSE_RANK() OVER (ORDER BY cost DESC) AS cost_dense_rank
    FROM with_last
),
latest_match AS (
    SELECT
        s.id,
        s.rating_avg_2_prior,
        s.cumulative_rating,
        s.last_known_rating_avg_2_prior,
        s.rating_rank_dense_rank,
        s.cumulative_cumulative_rating,
        s.cost_dense_rank,
        latest.product_price_2
    FROM enriched s
    JOIN LATERAL (
            SELECT c.price AS product_price_2
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    cumulative_rating,
    last_known_rating_avg_2_prior,
    rating_rank_dense_rank,
    cumulative_cumulative_rating,
    cost_dense_rank,
    product_price_2
FROM latest_match;
