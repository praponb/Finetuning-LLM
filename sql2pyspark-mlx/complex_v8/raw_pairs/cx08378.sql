WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE capacity BETWEEN 2614 AND 11506
      AND YEAR(opened_on) = 2024
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        CASE WHEN capacity >= 11506 THEN 'HIGH' WHEN capacity >= 4912 THEN 'MEDIUM' ELSE 'LOW' END AS capacity_tier,
        SUM(capacity) FILTER (WHERE city NOT IN ('Kuala Lumpur', 'Bangkok')) AS capacity_city_not_in_kuala_lumpur_ban,
        ROUND(STDDEV(capacity), 2) AS stddev_capacity
    FROM cleaned_warehouses
    GROUP BY city, CASE WHEN capacity >= 11506 THEN 'HIGH' WHEN capacity >= 4912 THEN 'MEDIUM' ELSE 'LOW' END
    HAVING MAX(capacity) > 2614
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(stddev_capacity) OVER (PARTITION BY capacity_tier, city) AS capacity_tier_city_max_stddev_capacity,
        FIRST_VALUE(stddev_capacity) OVER (PARTITION BY capacity_tier ORDER BY capacity_city_not_in_kuala_lumpur_ban DESC, city, capacity_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_stddev_capacity,
        ROUND(AVG(stddev_capacity) OVER (PARTITION BY capacity_tier, city), 2) AS capacity_tier_city_avg_stddev_capacity,
        NTILE(3) OVER (PARTITION BY capacity_tier ORDER BY capacity_city_not_in_kuala_lumpur_ban DESC, city, capacity_tier) AS capacity_city_not_in_kuala_lumpur_ban_tercile,
        ROUND(stddev_capacity - AVG(stddev_capacity) OVER w_capacity_tier_city, 2) AS stddev_capacity_vs_capacity_tier_city_avg
    FROM grouped
    WINDOW w_capacity_tier_city AS (PARTITION BY capacity_tier, city)
)
-- Native SQL UNPIVOT (wide to long)
SELECT city, capacity_tier, metric_name, metric_value
FROM (
    SELECT city, capacity_tier, capacity_tier_city_max_stddev_capacity, capacity_tier_city_avg_stddev_capacity
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (capacity_tier_city_max_stddev_capacity, capacity_tier_city_avg_stddev_capacity)
) unp
ORDER BY city, capacity_tier, metric_name
