WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM suppliers
    ) ranked
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IS NULL
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM not_flagged
    INTERSECT
    SELECT city, id
    FROM suppliers
    WHERE country = 'UK'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, country
    FROM kept_rows
    INTERSECT
    SELECT id, country
    FROM suppliers
    WHERE city = 'Singapore'
),
kept_rows_suppliers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN rating >= 4.4 THEN 'High' WHEN rating >= 1.8 THEN 'Medium' ELSE 'Low' END AS rating_tier,
        rating - rank_in_group AS rating_minus_rank_in_group,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM kept_rows_suppliers
    WHERE rating <= 3.1
)
-- Grouped metrics
SELECT
    city,
    AVG(rating) AS avg_rating,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(rating_minus_rank_in_group, ARRAY(0.25, 0.5, 0.75)) AS rating_minus_rank_in_group_quartiles,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM filtered
GROUP BY city
ORDER BY distinct_ids_2 DESC NULLS LAST;
