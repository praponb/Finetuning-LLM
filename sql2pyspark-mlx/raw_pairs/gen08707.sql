WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(opened_date, 53) AS opened_date_minus_53d
    FROM stores
    WHERE region IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM valid
    INTERSECT ALL
    SELECT id, city
    FROM stores
    WHERE sqft BETWEEN 7089 AND 11593
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
        )
),
eligible_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.quantity <= 75
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible_stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', opened_date_minus_53d) AS activity_month,
    SUM(CASE WHEN city = 'New York' THEN sqft ELSE 0 END) AS new_york_sqft,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2
FROM kept
GROUP BY DATE_TRUNC('MONTH', opened_date_minus_53d)
ORDER BY activity_month NULLS FIRST
