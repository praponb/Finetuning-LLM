WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM meter_readings
    WHERE quality IN ('OK', 'BAD')
      AND read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        quality,
        kwh_adj,
        LAG(read_at, 3) OVER (ORDER BY kwh DESC, id) AS prev3_read_at,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        LEAD(kwh_adj) OVER w_site_id_by_kwh AS next_kwh_adj,
        RANK() OVER (ORDER BY kwh DESC, id) AS kwh_rank,
        MIN(kwh) OVER w_quality_site_id AS quality_site_id_min_kwh
    FROM valid
    WINDOW w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh ASC, id), w_quality_site_id AS (PARTITION BY quality, site_id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        site_id,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        COUNT(*) AS cnt,
        ROUND(SUM(first_kwh), 2) AS total_first_kwh,
        SUM(CASE WHEN quality = 'EST' THEN first_kwh ELSE 0 END) AS first_kwh_quality_est,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM windowed
    GROUP BY site_id, DATE_TRUNC('MONTH', read_at)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        site_id,
        read_at_month,
        cnt,
        quality_list,
        ROUND(first_kwh_quality_est - AVG(first_kwh_quality_est) OVER w_cnt_read_at_month_month, 2) AS first_kwh_quality_est_vs_cnt_read_at_month_month,
        FIRST_VALUE(first_kwh_quality_est) IGNORE NULLS OVER (ORDER BY cnt DESC, site_id, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_kwh_quality_est,
        COUNT(*) OVER w_cnt_read_at_month_month AS rows_in_cnt_read_at_month_month,
        SUM(first_kwh_quality_est) OVER (PARTITION BY cnt, MONTH(read_at_month)) AS cnt_read_at_month_month_sum_first_kwh_quality_es
    FROM rollup_base
    WINDOW w_cnt_read_at_month_month AS (PARTITION BY cnt, MONTH(read_at_month))
)
SELECT
    site_id,
    cnt,
    quality_list,
    first_kwh_quality_est_vs_cnt_read_at_month_month,
    first_known_first_kwh_quality_est,
    rows_in_cnt_read_at_month_month,
    cnt_read_at_month_month_sum_first_kwh_quality_es,
    DATEDIFF(DATE '2026-01-01', read_at_month) AS days_since_read_at_month
FROM sequenced
ORDER BY site_id DESC
