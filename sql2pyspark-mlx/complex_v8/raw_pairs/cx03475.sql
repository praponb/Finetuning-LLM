WITH prepared_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM warehouses
    WHERE (city IN ('Singapore', 'Kuala Lumpur') OR capacity BETWEEN 2614 AND 11506)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        city,
        DATEDIFF(MAX(opened_on), MIN(opened_on)) AS opened_on_span_days,
        SUM(CASE WHEN city = 'Kuala Lumpur' THEN 1 ELSE 0 END) AS n_city_kuala_lumpur,
        SUM(capacity) AS total_capacity
    FROM prepared_warehouses
    GROUP BY city
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE total_capacity > (SELECT MIN(total_capacity) FROM rollup_base)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(city) OVER w_n_city_kuala_lumpur) AS distinct_city_in_n_city_kuala_lumpur,
        SUM(opened_on_span_days) OVER (PARTITION BY n_city_kuala_lumpur) AS n_city_kuala_lumpur_sum_opened_on_span_days
    FROM without_matches
    WINDOW w_n_city_kuala_lumpur AS (PARTITION BY n_city_kuala_lumpur)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        city,
        n_city_kuala_lumpur,
        n_city_kuala_lumpur_sum_opened_on_span_days,
        LEAD(n_city_kuala_lumpur) OVER (ORDER BY distinct_city_in_n_city_kuala_lumpur DESC NULLS FIRST, city) AS next_n_city_kuala_lumpur,
        LEAD(n_city_kuala_lumpur, 3, 0) OVER w_all_by_distinct_city_in_n_ci_2 AS next3_n_city_kuala_lumpur,
        ROW_NUMBER() OVER w_all_by_distinct_city_in_n_ci AS distinct_city_in_n_city_kuala_lumpur_seq,
        ROUND(AVG(total_capacity) OVER (PARTITION BY city), 2) AS city_avg_total_capacity,
        ROUND(n_city_kuala_lumpur / NULLIF(SUM(n_city_kuala_lumpur) OVER (PARTITION BY city), 0), 4) AS n_city_kuala_lumpur_share_of_city,
        MIN(opened_on_span_days) OVER (PARTITION BY city) AS city_min_opened_on_span_days
    FROM ranked
    WINDOW w_all_by_distinct_city_in_n_ci_2 AS (ORDER BY distinct_city_in_n_city_kuala_lumpur, city), w_all_by_distinct_city_in_n_ci AS (ORDER BY distinct_city_in_n_city_kuala_lumpur DESC NULLS FIRST, city)
)
SELECT
    distinct_city_in_n_city_kuala_lumpur_seq,
    n_city_kuala_lumpur,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(city)) AS city_list
FROM with_windows
GROUP BY GROUPING SETS ((distinct_city_in_n_city_kuala_lumpur_seq, n_city_kuala_lumpur), (distinct_city_in_n_city_kuala_lumpur_seq), ())
ORDER BY distinct_city_in_n_city_kuala_lumpur_seq NULLS LAST, n_city_kuala_lumpur NULLS LAST
