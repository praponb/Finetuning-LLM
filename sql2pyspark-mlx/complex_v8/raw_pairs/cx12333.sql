WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY MONTH(opened_on)), 2) AS capacity_vs_opened_on_month_avg,
        SIZE(COLLECT_SET(city) OVER w_opened_on_month) AS distinct_city_in_opened_on_month
    FROM prepared
    WINDOW w_opened_on_month AS (PARTITION BY MONTH(opened_on))
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        capacity_vs_opened_on_month_avg,
        distinct_city_in_opened_on_month,
        LAG(capacity) OVER w_city_or_unknown_by_capacity AS prev_capacity,
        MAX(capacity) OVER (PARTITION BY distinct_city_in_opened_on_month, city_or_unknown) AS distinct_city_in_opened_on_month_city_or_unknown,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY distinct_city_in_opened_on_month, city_or_unknown)) AS distinct_city_in_distinct_city_in_opened_on_mont,
        ROUND(AVG(capacity) OVER w_distinct_city_in_opened_on_m, 2) AS distinct_city_in_opened_on_month_city_or_unknown_2,
        NTH_VALUE(capacity, 2) OVER (PARTITION BY city_or_unknown ORDER BY capacity_vs_opened_on_month_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity,
        LAST_VALUE(capacity) OVER (PARTITION BY city_or_unknown ORDER BY capacity_vs_opened_on_month_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_capacity
    FROM with_trends
    WINDOW w_city_or_unknown_by_capacity AS (PARTITION BY city_or_unknown ORDER BY capacity_vs_opened_on_month_avg, id), w_distinct_city_in_opened_on_m AS (PARTITION BY distinct_city_in_opened_on_month, city_or_unknown)
),
grouped AS (
    -- Summarise per group
    SELECT
        city,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_ym,
        distinct_city_in_opened_on_month,
        ARRAY_SORT(COLLECT_SET(city)) AS city_list,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE LOWER(wh_code) LIKE '%nx%') AS n_lower_wh_code_like_nx,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(prev_capacity) AS total_prev_capacity
    FROM sequenced
    GROUP BY city, DATE_FORMAT(opened_on, 'yyyy-MM'), distinct_city_in_opened_on_month
)
SELECT city, opened_on_ym, measure, reading
FROM (
    SELECT city, opened_on_ym, total_prev_capacity, distinct_ids
    FROM grouped
) src
UNPIVOT (
    reading FOR measure IN (total_prev_capacity, distinct_ids)
) unp
ORDER BY city, opened_on_ym, measure
