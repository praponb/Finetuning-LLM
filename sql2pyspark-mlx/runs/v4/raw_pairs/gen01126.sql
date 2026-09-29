WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(country) AS clean_country,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IN ('Bangkok', 'Tokyo')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT country, id
    FROM suppliers
    WHERE country IN ('UK', 'DE', 'JP')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY rating DESC NULLS LAST) AS rating_rank,
        CUME_DIST() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS LAST) AS rating_cume_dist
    FROM reconciled
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price <= 490
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        country,
        city,
        COUNT(DISTINCT city) AS distinct_cities,
        MAX(rating) AS max_rating,
        ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS rating_per_singapore,
        COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows
    FROM kept
    GROUP BY country, city
    HAVING COUNT(DISTINCT city) < 1
)
SELECT
    country,
    city,
    distinct_cities,
    rating_per_singapore,
    tokyo_rows
FROM totals
ORDER BY country NULLS LAST, city DESC
