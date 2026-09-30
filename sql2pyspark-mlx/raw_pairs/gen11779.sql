WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        rating,
        UPPER(supplier_name) AS supplier_name_upper,
        TRIM(city) AS clean_city,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        supplier_name,
        city,
        rating,
        supplier_name_upper
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS FIRST, id) = 1
),
deduped_suppliers AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS FIRST, id) AS dedup_rank
        FROM deduped
    ) x
    WHERE dedup_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM deduped_suppliers
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE city NOT IN ('Tokyo', 'Bangkok', 'New York')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped_suppliers s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category = 'Books'
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN rating >= 4.2 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    SUM(CASE WHEN city = 'Bangkok' THEN rating ELSE 0 END) AS bangkok_rating,
    ROUND(AVG(rating), 2) AS avg_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(rating) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_total_rating
FROM eligible
GROUP BY CASE WHEN rating >= 4.2 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY bangkok_rating DESC;
