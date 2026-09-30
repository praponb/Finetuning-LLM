WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM meter_readings
    WHERE voltage < 240.2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY read_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        LAG(read_at) OVER w_all_by_read_at AS prev_read_at,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY read_at_hour DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        NTH_VALUE(read_at, 2) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        SUM(kwh) OVER (PARTITION BY MONTH(read_at_hour), site_id) AS read_at_hour_month_site_id_sum_kwh,
        NTILE(5) OVER w_all_by_read_at_2 AS read_at_quintile
    FROM scoped
    WINDOW w_all_by_read_at AS (ORDER BY read_at DESC, id), w_site_id AS (PARTITION BY site_id), w_all_by_read_at_2 AS (ORDER BY read_at ASC, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        read_at_quintile,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        MIN(prev_read_at) AS first_prev_read_at,
        ROUND(100 * COUNT(CASE WHEN quality NOT IN ('OK', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_ok_bad
    FROM metrics
    GROUP BY read_at_quintile, DATE_TRUNC('MONTH', read_at)
    HAVING MAX(kwh_avg_2_prior) > 12.0
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(first_prev_read_at) OVER (PARTITION BY read_at_quintile ORDER BY pct_quality_not_in_ok_bad DESC, read_at_quintile, read_at_month) AS prev_first_prev_read_at,
        LAST_VALUE(read_at_quintile) IGNORE NULLS OVER (PARTITION BY YEAR(read_at_month) ORDER BY first_prev_read_at DESC, read_at_quintile, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at_quintile,
        LAST_VALUE(read_at_month) OVER (PARTITION BY read_at_quintile ORDER BY pct_quality_not_in_ok_bad DESC, read_at_quintile, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_month,
        LAST_VALUE(read_at_month) IGNORE NULLS OVER (PARTITION BY YEAR(read_at_month) ORDER BY first_prev_read_at DESC, read_at_quintile, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at_month
    FROM grouped
)
SELECT
    read_at_quintile,
    read_at_month,
    pct_quality_not_in_ok_bad,
    last_known_read_at_quintile,
    last_known_read_at_month,
    DATEDIFF(DATE '2026-01-01', read_at_month) AS days_since_read_at_month
FROM metrics_2
ORDER BY last_known_read_at_quintile DESC
