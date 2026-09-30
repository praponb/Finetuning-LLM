WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(city) AS city_lower,
        INITCAP(TRIM(city)) AS city_title
    FROM suppliers
    WHERE city IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
ranked_suppliers AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IS NOT NULL
        )
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND YEAR(ch.launch_date) >= 2025
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city_title,
        rank_in_group,
        NTILE(4) OVER (PARTITION BY country, city ORDER BY rating DESC, id) AS rating_bucket,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM eligible_suppliers
    WHERE city <> 'Chiang Mai'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city_title,
        rank_in_group,
        rating_bucket,
        cumulative_rating,
        CONCAT(country, ' - ', supplier_name) AS country_supplier_name
    FROM enriched
    WHERE rating_bucket BETWEEN 3 AND 8
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_rating >= 19 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_rating_tier,
    ROUND(AVG(rating_bucket), 2) AS avg_rating_bucket,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    ROUND(AVG(rating_bucket), 2) AS avg_rating_bucket_2
FROM valid
GROUP BY 1
