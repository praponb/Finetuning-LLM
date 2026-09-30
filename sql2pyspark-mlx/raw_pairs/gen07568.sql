WITH remaining_keys AS (
    SELECT id, country
    FROM suppliers
    INTERSECT
    SELECT id, country
    FROM suppliers
    WHERE country IN ('TH', 'DE')
),
kept_rows AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
)
SELECT
    CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    country,
    city,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS rating_per_sg,
    GROUPING_ID() AS gid
FROM kept_rows
GROUP BY GROUPING SETS ((CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END, country, city), (CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END, country));
