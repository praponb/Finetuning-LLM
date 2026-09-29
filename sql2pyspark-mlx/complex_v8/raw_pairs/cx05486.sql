WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        INITCAP(TRIM(wh_code)) AS wh_code_clean
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
      AND wh_code IS NOT NULL
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        city,
        ROUND(STDDEV(capacity), 2) AS stddev_capacity,
        COUNT(*) FILTER (WHERE UPPER(TRIM(wh_code)) LIKE 'B%') AS n_upper_trim_wh_code_like_b,
        ROUND(AVG(capacity), 2) AS avg_capacity,
        SUM(capacity) FILTER (WHERE city IS NOT NULL) AS capacity_city_is_not_null,
        COUNT(DISTINCT city) AS distinct_citys
    FROM scoped
    GROUP BY 1, 2
    HAVING MAX(capacity) > 2614
),
annotated AS (
    -- Label and score rows
    SELECT
        opened_on_month,
        city,
        stddev_capacity,
        avg_capacity,
        distinct_citys,
        COALESCE(distinct_citys, 0) AS distinct_citys_filled,
        CASE WHEN distinct_citys >= 7 THEN 'HIGH' WHEN distinct_citys >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_citys_tier,
        avg_capacity + stddev_capacity AS avg_capacity_plus_stddev_capacity
    FROM stats
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE avg_capacity_plus_stddev_capacity > (SELECT AVG(avg_capacity_plus_stddev_capacity) FROM annotated)
)
-- Final projection
SELECT
    opened_on_month,
    city,
    stddev_capacity,
    distinct_citys,
    distinct_citys_filled,
    distinct_citys_tier,
    avg_capacity_plus_stddev_capacity,
    DATEDIFF(DATE '2026-01-01', opened_on_month) AS days_since_opened_on_month,
    GREATEST(distinct_citys, stddev_capacity) AS greatest_distinct_citys_stddev_capacity
FROM with_activity
ORDER BY avg_capacity_plus_stddev_capacity, distinct_citys DESC
