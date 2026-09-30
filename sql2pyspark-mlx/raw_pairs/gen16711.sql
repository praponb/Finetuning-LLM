WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        rating,
        UPPER(city) AS city_upper,
        CASE WHEN country IN ('UK', 'US') THEN 'Core' ELSE 'Other' END AS country_group,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE country = 'DE'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        city_upper,
        resolved_city,
        FIRST_VALUE(rating) IGNORE NULLS OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM valid
),
valid_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        city_upper,
        first_known_rating,
        cumulative_rating,
        running_rating,
        first_known_rating - cumulative_rating AS first_known_rating_minus_cumulative_rating,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM metrics
    WHERE city IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid_suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY first_known_rating_minus_cumulative_rating DESC NULLS FIRST, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN first_known_rating_minus_cumulative_rating >= 84 THEN 'HIGH' ELSE 'LOW' END AS first_known_rating_minus_cumulative_rating_tier,
    SUM(CASE WHEN city = 'London' THEN first_known_rating_minus_cumulative_rating ELSE 0 END) AS london_first_known_rating_minus_cumulative_rating,
    COUNT(*) AS row_count,
    ROUND(SUM(first_known_rating_minus_cumulative_rating) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS first_known_rating_minus_cumulative_rating_per_bangkok,
    SUM(CASE WHEN city = 'New York' THEN first_known_rating_minus_cumulative_rating ELSE 0 END) AS new_york_first_known_rating_minus_cumulative_rating,
    APPROX_PERCENTILE(first_known_rating_minus_cumulative_rating, 0.95) AS first_known_rating_minus_cumulative_rating_p95,
    SUM(CASE WHEN city = 'Berlin' THEN cumulative_rating ELSE 0 END) AS berlin_cumulative_rating,
    GROUPING(city) AS grouping_level
FROM ranked
GROUP BY ROLLUP (city, CASE WHEN first_known_rating_minus_cumulative_rating >= 84 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY city DESC;
