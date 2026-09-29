WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE site_id <= 7
      AND quality IN ('BAD', 'OK')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        FIRST_VALUE(read_at_week) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at_week,
        MAX(kwh) OVER (ORDER BY read_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        MIN(voltage) OVER w_site_id AS site_id_min_voltage,
        NTILE(10) OVER (ORDER BY read_at_week DESC, id) AS read_at_week_decile,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        DENSE_RANK() OVER w_site_id_by_kwh_ties AS kwh_dense_rank
    FROM filtered_readings
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_by_kwh_ties AS (PARTITION BY site_id ORDER BY kwh)
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        MIN(max_kwh_so_far) AS min_max_kwh_so_far,
        COUNT(*) FILTER (WHERE quality NOT IN ('EST', 'BAD')) AS n_quality_not_in_est_bad
    FROM windowed
    WHERE site_id >= 5
    GROUP BY 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER w_n_quality_not_in_est_bad_sit AS min_max_kwh_so_far_quintile,
        LAST_VALUE(min_max_kwh_so_far) OVER (PARTITION BY n_quality_not_in_est_bad, site_id ORDER BY min_max_kwh_so_far, site_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_min_max_kwh_so_far
    FROM per_group
    WINDOW w_n_quality_not_in_est_bad_sit AS (PARTITION BY n_quality_not_in_est_bad, site_id ORDER BY min_max_kwh_so_far, site_id)
)
SELECT
    site_id,
    min_max_kwh_so_far,
    n_quality_not_in_est_bad,
    min_max_kwh_so_far_quintile
FROM with_trends
WHERE last_min_max_kwh_so_far <= 840.0
ORDER BY site_id NULLS LAST, n_quality_not_in_est_bad DESC
