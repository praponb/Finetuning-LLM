WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        quality,
        SUM(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_kwh,
        LAST_VALUE(read_at) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        ROUND(kwh - AVG(kwh) OVER w_quality, 2) AS kwh_vs_quality_avg,
        LAG(read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY kwh, id) AS prev_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY kwh, id) AS kwh_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY kwh, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_read_at_year_by_read_at)) / 60, 1) AS minutes_since_prev_read_at,
        NTILE(3) OVER (PARTITION BY quality, site_id ORDER BY kwh, id) AS kwh_tercile
    FROM scoped_readings
    WINDOW w_quality AS (PARTITION BY quality), w_read_at_year_by_read_at AS (PARTITION BY YEAR(read_at) ORDER BY read_at, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY kwh_tercile, quality)) AS distinct_site_id_in_kwh_tercile_quality,
        DENSE_RANK() OVER w_all_by_kwh_dense_rank_ties AS kwh_dense_rank_dense_rank,
        MAX(quality_site_id_sum_kwh) OVER (ORDER BY kwh_dense_rank ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS quality_site_id_sum_kwh_max_2,
        ROUND(kwh_vs_quality_avg / NULLIF(SUM(kwh_vs_quality_avg) OVER w_kwh_tercile_quality, 0), 4) AS kwh_vs_quality_avg_share_of_kwh_tercile_quality,
        RANK() OVER (PARTITION BY site_id, kwh_tercile ORDER BY minutes_since_prev_read_at DESC) AS minutes_since_prev_read_at_rank
    FROM with_windows
    WINDOW w_all_by_kwh_dense_rank_ties AS (ORDER BY kwh_dense_rank ASC), w_kwh_tercile_quality AS (PARTITION BY kwh_tercile, quality)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality,
    quality_site_id_sum_kwh,
    last_read_at,
    kwh_vs_quality_avg,
    prev_read_at,
    kwh_vs_quality_site_id_avg,
    distinct_site_id_in_kwh_tercile_quality,
    quality_site_id_sum_kwh_max_2,
    minutes_since_prev_read_at_rank,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
    COALESCE(kwh_vs_quality_avg, 0.0) AS kwh_vs_quality_avg_filled
FROM ranked
ORDER BY kwh_vs_quality_site_id_avg NULLS LAST, kwh_vs_quality_avg NULLS LAST
