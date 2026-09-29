WITH valid_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on,
        ABS(capacity - 4912) AS capacity_distance,
        LAST_DAY(opened_on) AS opened_on_month_end
    FROM warehouses
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        city,
        capacity,
        opened_on,
        capacity_distance,
        opened_on_month_end,
        ADD_MONTHS(opened_on, 1) AS opened_on_plus_1m,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM valid_warehouses
    WHERE city NOT IN ('Singapore', 'Bangkok')
),
city_or_unknown_stats AS (
    -- Avg capacity per city_or_unknown
    SELECT city_or_unknown, AVG(capacity) AS avg_capacity
    FROM tagged
    GROUP BY city_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_capacity,
        ROUND(s.capacity - g.avg_capacity, 2) AS capacity_minus_avg
    FROM tagged s
    JOIN city_or_unknown_stats g ON g.city_or_unknown = s.city_or_unknown
    WHERE s.capacity > g.avg_capacity
)
SELECT
    city,
    city_or_unknown,
    GROUPING(city) AS is_total_city,
    ARRAY_SORT(COLLECT_SET(city)) AS city_list,
    ROUND(SUM(capacity_minus_avg), 2) AS total_capacity_minus_avg
FROM compared
GROUP BY GROUPING SETS ((city, city_or_unknown), (city), (city_or_unknown), ())
ORDER BY city NULLS LAST, city_or_unknown NULLS LAST
