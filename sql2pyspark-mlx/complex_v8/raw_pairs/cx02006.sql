WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LOWER(TRIM(quality)) AS quality_clean,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE voltage <= 233.2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_site_id_by_read_at_year, 3) AS read_at_year_percent_rank,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), read_at_year), 2) AS kwh_vs_read_at_month_read_at_year_avg,
        ROUND(read_at_year - AVG(read_at_year) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), read_at_year), 2) AS read_at_year_vs_read_at_month_read_at_year_avg,
        ROUND(read_at_year / NULLIF(SUM(read_at_year) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), read_at_year), 0), 4) AS read_at_year_share_of_read_at_month_read_at_year,
        SUM(kwh) OVER w_read_at_month_read_at_year AS read_at_month_read_at_year_sum_kwh,
        LAST_VALUE(read_at) OVER (PARTITION BY read_at_year, quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), read_at_year)) AS distinct_site_id_in_read_at_month_read_at_year
    FROM cleaned_readings
    WINDOW w_site_id_by_read_at_year AS (PARTITION BY site_id ORDER BY read_at_year DESC, id), w_read_at_month_read_at_year AS (PARTITION BY DATE_TRUNC('MONTH', read_at), read_at_year)
),
annotated AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(quality)) AS quality_len
    FROM metrics
    WHERE last_read_at >= TIMESTAMP '2025-01-19 07:00:00'
),
stats AS (
    -- Group-level totals
    SELECT
        quality,
        site_id,
        ROUND(SUM(read_at_year_percent_rank), 2) AS total_read_at_year_percent_rank,
        MAX(last_read_at) AS last_last_read_at,
        COUNT(*) FILTER (WHERE quality NOT IN ('BAD', 'EST')) AS n_quality_not_in_bad_est
    FROM annotated
    GROUP BY quality, site_id
)
-- Final projection
SELECT
    quality,
    site_id,
    total_read_at_year_percent_rank,
    last_last_read_at,
    n_quality_not_in_bad_est,
    CONCAT(quality, '#', CAST(n_quality_not_in_bad_est AS STRING)) AS quality_ref,
    DAYOFWEEK(last_last_read_at) AS last_last_read_at_dayofweek
FROM stats
WHERE n_quality_not_in_bad_est <= 8
ORDER BY n_quality_not_in_bad_est DESC NULLS FIRST, total_read_at_year_percent_rank, quality, site_id
LIMIT 5
