WITH kept_keys AS (
    -- Set difference on keys
    SELECT city, country
    FROM suppliers
    EXCEPT
    SELECT city, country
    FROM suppliers
    WHERE city = 'New York'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN kept_keys k ON s.city = k.city AND s.country = k.country
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name LIKE '%Off%'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category = 'Fashion'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        supplier_name,
        country,
        rating,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_cume_dist,
        PERCENT_RANK() OVER (ORDER BY rating NULLS FIRST) AS rating_percent_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM kept
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(rating * 0.9, 2) AS adjusted_rating
    FROM metrics
    WHERE country IN ('US', 'JP', 'TH')
)
SELECT
    country,
    rating,
    rating_cume_dist,
    rating_share
FROM filtered
ORDER BY rating_cume_dist NULLS FIRST, rating_share DESC;
