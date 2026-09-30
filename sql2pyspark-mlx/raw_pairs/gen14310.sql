WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN rating >= 4.6 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN city = 'London' THEN rating ELSE 0 END) AS london_rating
FROM kept
GROUP BY GROUPING SETS ((city, CASE WHEN rating >= 4.6 THEN 'HIGH' ELSE 'LOW' END), (city))
