WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        rating,
        SUBSTRING(country, 1, 4) country_prefix
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS NOT DISTINCT FROM 'New York'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.cost
    FROM valid s
    JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        LAG(cost, 2) OVER (ORDER BY cost, id) prev_cost,
        LAG(rating, 1) OVER (ORDER BY cost, id) prev_rating,
        LEAD(cost, 1) OVER (ORDER BY cost, id) next_cost,
        FIRST_VALUE(rating, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_rating
    FROM latest_match
    WHERE rating > 4.0
),
latest_match_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.prev_cost,
        s.first_known_rating,
        x.launch_date,
        x.product_brand
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.launch_date, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        prev_cost,
        first_known_rating,
        SUM(prev_cost) OVER (PARTITION BY product_brand ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_prev_cost,
        AVG(first_known_rating) OVER (PARTITION BY product_brand ORDER BY prev_cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) first_known_rating_avg_2_prior,
        MIN(first_known_rating) OVER (PARTITION BY product_brand) product_brand_min_first_known_rating,
        AVG(first_known_rating) OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date), product_brand ORDER BY first_known_rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) first_known_rating_avg_2,
        DENSE_RANK() OVER (PARTITION BY product_brand ORDER BY first_known_rating DESC NULLS LAST) first_known_rating_dense_rank
    FROM latest_match_suppliers
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.first_known_rating,
        s.product_brand_min_first_known_rating,
        s.first_known_rating_dense_rank,
        x.product_launch_date,
        x.category
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(first_known_rating) OVER (PARTITION BY category ORDER BY product_brand_min_first_known_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_first_known_rating,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY product_launch_date, id) event_seq,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', product_launch_date), category ORDER BY first_known_rating_dense_rank DESC NULLS LAST) first_known_rating_dense_rank_dense_rank,
        ROW_NUMBER() OVER (ORDER BY first_known_rating DESC, id) row_seq
    FROM with_last
)
SELECT
    id,
    product_brand_min_first_known_rating,
    first_known_rating_dense_rank,
    category,
    cumulative_first_known_rating,
    event_seq,
    row_seq,
    CASE WHEN category RLIKE '^(Books|Sports)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS category_zone
FROM metrics
ORDER BY id NULLS LAST
LIMIT 20
