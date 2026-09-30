WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(rating * 1.07, 2) AS adjusted_rating,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM suppliers
    WHERE rating < 4.1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand IN ('Acme', 'Stark')
        )
),
valid_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(adjusted_rating, rating) AS least_adjusted_rating_rating,
        COALESCE(city, 'DIRECT') AS resolved_city_2
    FROM kept
    WHERE city IS NOT NULL
      AND adjusted_rating <= 50
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand = 'Umbrella'
        )
)
-- Multi-level aggregation
SELECT
    country,
    CASE WHEN rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    ROUND(AVG(least_adjusted_rating_rating), 2) AS avg_least_adjusted_rating_rating,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM kept_suppliers
GROUP BY ROLLUP (country, CASE WHEN rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END);
