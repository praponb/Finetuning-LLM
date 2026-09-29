WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        DATEDIFF(DATE '2025-06-30', opened_on) AS days_since_opened_on,
        CASE WHEN capacity >= 11506 THEN 'large' WHEN capacity >= 4912 THEN 'medium' ELSE 'small' END AS capacity_tier
    FROM warehouses
    WHERE capacity > 11005
      AND UPPER(TRIM(wh_code)) LIKE 'C%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        city,
        opened_on,
        days_since_opened_on,
        MAX(days_since_opened_on) OVER (PARTITION BY capacity_tier) AS capacity_tier_max_days_since_opened_on,
        LAG(opened_on) OVER (ORDER BY days_since_opened_on DESC, id) AS prev_opened_on,
        SUM(days_since_opened_on) OVER (ORDER BY days_since_opened_on DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_opened_on_sum_2,
        DATEDIFF(opened_on, LAG(opened_on) OVER (ORDER BY days_since_opened_on DESC, id)) AS days_since_prev_opened_on,
        ROUND(AVG(days_since_opened_on) OVER (ORDER BY days_since_opened_on DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_opened_on_avg_2
    FROM prepared
),
totals AS (
    -- Group-level totals
    SELECT
        city,
        YEAR(prev_opened_on) AS prev_opened_on_year,
        SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS n_city_singapore,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT city) AS distinct_citys,
        ARRAY_SORT(COLLECT_SET(city)) AS city_list,
        DATEDIFF(MAX(opened_on), MIN(opened_on)) AS opened_on_span_days
    FROM metrics
    GROUP BY city, YEAR(prev_opened_on)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        city,
        prev_opened_on_year,
        city_list,
        opened_on_span_days,
        NTILE(3) OVER (PARTITION BY n_city_singapore ORDER BY distinct_citys NULLS LAST, city, prev_opened_on_year) AS distinct_citys_tercile,
        SIZE(COLLECT_SET(city) OVER w_prev_opened_on_year) AS distinct_city_in_prev_opened_on_year,
        ROUND(n_city_singapore / NULLIF(SUM(n_city_singapore) OVER (PARTITION BY prev_opened_on_year), 0), 4) AS n_city_singapore_share_of_prev_opened_on_year,
        ROUND(prev_opened_on_year / NULLIF(SUM(prev_opened_on_year) OVER (PARTITION BY prev_opened_on_year), 0), 4) AS prev_opened_on_year_share_of_prev_opened_on_year,
        MIN(n_city_singapore) OVER (PARTITION BY prev_opened_on_year) AS prev_opened_on_year_min_n_city_singapore,
        COUNT(*) OVER (PARTITION BY prev_opened_on_year) AS rows_in_prev_opened_on_year
    FROM totals
    WINDOW w_prev_opened_on_year AS (PARTITION BY prev_opened_on_year)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(city, '#', CAST(prev_opened_on_year AS STRING)) AS city_ref,
        ARRAY_JOIN(ARRAY_SORT(city_list), ',') AS city_list_text,
        SUBSTRING(TRIM(city), 1, 2) AS city_prefix
    FROM ranked
)
SELECT
    city,
    prev_opened_on_year,
    city_list,
    opened_on_span_days,
    distinct_citys_tercile,
    distinct_city_in_prev_opened_on_year,
    prev_opened_on_year_share_of_prev_opened_on_year,
    prev_opened_on_year_min_n_city_singapore,
    rows_in_prev_opened_on_year,
    city_ref
FROM annotated
ORDER BY opened_on_span_days, city_ref DESC
