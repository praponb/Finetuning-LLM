WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE quality IN ('OK', 'BAD')
      AND read_at >= TIMESTAMP '2025-01-25 13:00:00'
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_month,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY read_at_month ASC, id), 3) AS read_at_month_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY read_at_month, id), 3) AS read_at_month_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage DESC), 3) AS voltage_percent_rank,
        COUNT(*) OVER (PARTITION BY read_at_month) AS rows_in_read_at_month,
        MAX(voltage) OVER w_read_at_month AS read_at_month_max_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY read_at_month ASC), 3) AS read_at_month_percent_rank_2,
        LEAD(read_at_month) OVER (PARTITION BY site_id, site_zone ORDER BY read_at_month, id) AS next_read_at_month,
        MAX(kwh) OVER (ORDER BY read_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far
    FROM with_parent_site
    WINDOW w_read_at_month AS (PARTITION BY read_at_month)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        read_at_month,
        site_installed_kw,
        read_at_month_percent_rank,
        read_at_month_cume_dist,
        voltage_percent_rank,
        read_at_month_max_voltage,
        read_at_month_percent_rank_2,
        voltage + max_kwh_so_far AS voltage_plus_max_kwh_so_far
    FROM with_windows
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        read_at_month_percent_rank_2 - LAG(read_at_month_percent_rank_2) OVER (PARTITION BY site_id ORDER BY read_at_month_percent_rank ASC, id) AS read_at_month_percent_rank_2_change,
        NTH_VALUE(read_at_month_percent_rank_2, 2) OVER (PARTITION BY quality, read_at_month ORDER BY read_at_month_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_month_percent_rank_2,
        MAX(read_at_month_max_voltage) OVER (PARTITION BY quality ORDER BY voltage_percent_rank DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS read_at_month_max_voltage_max_3_prior,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY read_at_month, quality)) AS distinct_site_id_in_read_at_month_quality
    FROM scored
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        read_at_month,
        site_id,
        MAX(read_at) AS last_read_at,
        SUM(read_at_month_max_voltage_max_3_prior) AS total_read_at_month_max_voltage_max_3_prior,
        SUM(CASE WHEN quality IN ('OK', 'EST') THEN 1 ELSE 0 END) AS n_quality_in_ok_est,
        COUNT(*) FILTER (WHERE quality <> 'OK') AS n_quality_ok
    FROM windowed
    WHERE quality = 'BAD'
    GROUP BY 1, 2
    HAVING MAX(voltage_plus_max_kwh_so_far) > 433.5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(last_read_at, 2) OVER (PARTITION BY site_id ORDER BY last_read_at, read_at_month, site_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_read_at,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY last_read_at, read_at_month, site_id) AS last_read_at_decile,
        LEAD(last_read_at) OVER (PARTITION BY site_id ORDER BY last_read_at, read_at_month, site_id) AS next_last_read_at,
        ROUND(AVG(n_quality_in_ok_est) OVER (PARTITION BY site_id ORDER BY last_read_at, read_at_month, site_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS n_quality_in_ok_est_avg_3_prior
    FROM grouped
)
SELECT
    read_at_month,
    site_id,
    last_read_at,
    total_read_at_month_max_voltage_max_3_prior,
    second_last_read_at,
    ROUND((UNIX_TIMESTAMP(next_last_read_at) - UNIX_TIMESTAMP(second_last_read_at)) / 60, 1) AS minutes_second_last_read_at_to_next_last_read_at
FROM ranked
WHERE last_read_at_decile > 2
ORDER BY site_id DESC
