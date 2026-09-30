WITH kept_keys AS (
    -- Set difference on keys
    SELECT city
    FROM suppliers
    EXCEPT DISTINCT
    SELECT city
    FROM suppliers
    WHERE rating <= 1.8
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.city = k.city
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost < 630
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        rating,
        REPLACE(supplier_name, ' ', '') AS supplier_name_slug,
        COALESCE(rating, 0) AS rating_filled,
        COALESCE(city, 'OTHER') AS city_filled
    FROM kept
    WHERE city IS NOT NULL
      AND LOWER(TRIM(supplier_name)) = 'pacific wholesale'
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM valid
    EXCEPT ALL
    SELECT country, id
    FROM suppliers
    WHERE city = 'Bangkok'
),
reconciled_suppliers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.country = k.country AND s.id = k.id
),
valid_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        country,
        rating,
        rating_filled,
        city_filled,
        SUBSTRING(country, 1, 2) AS country_prefix,
        SUBSTRING(city, 1, 4) AS city_prefix
    FROM reconciled_suppliers
    WHERE city IS NOT DISTINCT FROM 'New York'
      AND rating_filled < 54
)
-- Grouped metrics
SELECT
    CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(*) AS row_count
FROM valid_suppliers
GROUP BY CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END
ORDER BY row_count DESC NULLS LAST;
