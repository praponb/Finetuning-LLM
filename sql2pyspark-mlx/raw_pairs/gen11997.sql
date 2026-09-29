WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        SUBSTRING(country, 1, 3) AS country_prefix,
        ROUND(rating, 2) AS rating_rounded,
        COALESCE(city, 'NONE') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating <= 3.0
),
cleaned_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(supplier_name) AS clean_supplier_name,
        COALESCE(city, 'OTHER') AS resolved_city_2
    FROM cleaned
    WHERE city IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS dedup_rank
        FROM cleaned_suppliers
    ) t
    WHERE dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id = 30
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    city,
    SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) AS event_count
FROM not_flagged
GROUP BY GROUPING SETS ((CASE WHEN rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END, city), (CASE WHEN rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING COUNT(*) >= 1
ORDER BY jp_rating DESC
