WITH filtered_stores AS (
    SELECT *, CASE WHEN region = 'Central' THEN 1 ELSE 0 END AS is_central
    FROM stores
    WHERE region = 'North'
),
agg AS (
    SELECT region, SUM(is_central) AS total_is_central, MIN(opened_date) AS first_opened_date
    FROM filtered_stores
    GROUP BY region
)
SELECT *
FROM agg
WHERE total_is_central > 200;
