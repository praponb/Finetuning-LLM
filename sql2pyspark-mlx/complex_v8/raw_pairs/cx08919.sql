WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE product = 'HOME'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(start_date_year, 'yyyy-MM') AS start_date_year_ym,
        YEAR(start_date) AS start_date_year_2,
        COUNT(*) AS cnt,
        COUNT(DISTINCT status) AS distinct_status,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM cleaned
    WHERE HOUR(start_date_year) BETWEEN 6 AND 12
    GROUP BY 1, 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        start_date_year_ym,
        start_date_year_2,
        cnt,
        ROUND(start_date_year_2 / NULLIF(SUM(start_date_year_2) OVER (PARTITION BY cnt, start_date_year_2), 0), 4) AS start_date_year_2_share_of_cnt_start_date_year_2,
        NTH_VALUE(start_date_year_2, 2) OVER (PARTITION BY distinct_status ORDER BY distinct_status, start_date_year_ym, start_date_year_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_year_2,
        LAG(distinct_status) OVER (PARTITION BY distinct_status ORDER BY distinct_status, start_date_year_ym, start_date_year_2) AS prev_distinct_status,
        LAST_VALUE(cnt) OVER (PARTITION BY start_date_year_2 ORDER BY distinct_status ASC, start_date_year_ym, start_date_year_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_cnt,
        LEAD(distinct_status) OVER w_distinct_status_by_distinct_2 AS next_distinct_status,
        FIRST_VALUE(distinct_status) IGNORE NULLS OVER (PARTITION BY start_date_year_2 ORDER BY distinct_status ASC, start_date_year_ym, start_date_year_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_status
    FROM per_group
    WINDOW w_distinct_status_by_distinct_2 AS (PARTITION BY distinct_status ORDER BY distinct_status, start_date_year_ym, start_date_year_2)
)
SELECT start_date_year_ym, start_date_year_2, metric_name, amount
FROM (
    SELECT start_date_year_ym, start_date_year_2, second_start_date_year_2, first_known_distinct_status
    FROM with_trends
) src
UNPIVOT (
    amount FOR metric_name IN (second_start_date_year_2, first_known_distinct_status)
) unp
ORDER BY start_date_year_ym, start_date_year_2, metric_name
