WITH base_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        ADD_MONTHS(opened_on, 3) AS opened_on_plus_3m
    FROM warehouses
    WHERE capacity > 2614
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN city IN ('Singapore', 'Kuala Lumpur') THEN 'group_a' WHEN city = 'Bangkok' THEN 'group_b' ELSE 'other' END AS city_group,
        CONCAT_WS('|', city, wh_code) AS city_wh_code_label,
        CONCAT(city, '#', CAST(capacity AS STRING)) AS city_ref
    FROM base_warehouses
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN capacity >= 11506 THEN 'large' WHEN capacity >= 4912 THEN 'medium' ELSE 'small' END AS capacity_tier,
        UPPER(TRIM(city)) AS city_clean,
        DATE_TRUNC('YEAR', opened_on_plus_3m) AS opened_on_plus_3m_year
    FROM annotated
    WHERE city_ref IS NOT NULL
),
city_group_stats AS (
    -- Avg capacity per city_group
    SELECT city_group, AVG(capacity) AS avg_capacity
    FROM annotated_2
    GROUP BY city_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_capacity,
        ROUND(s.capacity - g.avg_capacity, 2) AS capacity_minus_avg
    FROM annotated_2 s
    JOIN city_group_stats g ON g.city_group = s.city_group
    WHERE s.capacity > g.avg_capacity
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', opened_on_plus_3m) AS opened_on_plus_3m_month,
        city,
        ROUND(SUM(capacity) / NULLIF(SUM(capacity_minus_avg), 0), 3) AS capacity_to_capacity_minus_avg_ratio,
        ROUND(SUM(avg_capacity) / NULLIF(SUM(capacity_minus_avg), 0), 3) AS avg_capacity_to_capacity_minus_avg_ratio,
        SUM(avg_capacity) AS total_avg_capacity,
        MIN(avg_capacity) AS min_avg_capacity,
        MAX(capacity_minus_avg) AS max_capacity_minus_avg
    FROM vs_group
    GROUP BY DATE_TRUNC('MONTH', opened_on_plus_3m), city
    HAVING MAX(capacity_minus_avg) > -8000.0
),
tagged AS (
    -- Derive helper columns
    SELECT
        opened_on_plus_3m_month,
        city,
        capacity_to_capacity_minus_avg_ratio,
        total_avg_capacity,
        min_avg_capacity,
        max_capacity_minus_avg,
        LEAST(min_avg_capacity, max_capacity_minus_avg) AS least_min_avg_capacity_max_capacity_minus_avg
    FROM rollup_base
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM tagged
    WHERE max_capacity_minus_avg < (SELECT MAX(max_capacity_minus_avg) FROM tagged)
)
SELECT
    opened_on_plus_3m_month,
    city,
    total_avg_capacity,
    min_avg_capacity,
    max_capacity_minus_avg
FROM with_activity
WHERE city IN ('Bangkok', 'Kuala Lumpur')
ORDER BY city DESC, min_avg_capacity
