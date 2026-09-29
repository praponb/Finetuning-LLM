WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        LENGTH(city) AS city_length
    FROM suppliers
    WHERE city IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY city_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS event_seq,
        LAG(city_length, 1) OVER (PARTITION BY city ORDER BY rating, id) AS prev_city_length
    FROM cleaned
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost >= 510
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.rating,
        s.first_known_city,
        s.event_seq,
        s.prev_city_length,
        top1.product_category
    FROM eligible s
    JOIN LATERAL (
            SELECT c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        rating,
        first_known_city,
        event_seq,
        prev_city_length,
        ROUND(prev_city_length / rating, 2) AS prev_city_length_per_rating,
        COALESCE(product_category, 'UNKNOWN') AS resolved_product_category
    FROM latest_match
    WHERE product_category IN ('Sports', 'Electronics')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        rating,
        first_known_city,
        prev_city_length,
        prev_city_length_per_rating,
        resolved_product_category,
        LEAST(prev_city_length_per_rating, prev_city_length) AS least_prev_city_length_per_rating_prev_city_length
    FROM cleaned_suppliers
    WHERE prev_city_length_per_rating < 21
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(3) OVER (ORDER BY least_prev_city_length_per_rating_prev_city_length, id) AS least_prev_city_length_per_rating_prev_city_length_bucket,
    SUM(prev_city_length_per_rating) OVER (ORDER BY prev_city_length_per_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_city_length_per_rating,
    LEAD(prev_city_length_per_rating, 2) OVER (ORDER BY prev_city_length, id) AS next_prev_city_length_per_rating
FROM filtered
WHERE prev_city_length_per_rating < 51;
