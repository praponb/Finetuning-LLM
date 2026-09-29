WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality <> 'EST'
      AND kwh > 26.9
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS voltage_vs_quality_site_id_avg,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_id), 0), 4) AS kwh_share_of_quality_site_id,
        NTILE(3) OVER (ORDER BY kwh DESC, id) AS kwh_tercile,
        ROW_NUMBER() OVER (ORDER BY kwh DESC, id) AS seq_no,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_voltage,
        SUM(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        MAX(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_kwh,
        MAX(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        ROUND(CUME_DIST() OVER w_all_by_kwh, 3) AS kwh_cume_dist,
        RANK() OVER (ORDER BY kwh DESC) AS kwh_rank
    FROM prepared
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        rows_in_quality_site_id,
        voltage_vs_quality_site_id_avg,
        kwh_share_of_quality_site_id,
        kwh_tercile,
        quality_site_id_avg_voltage,
        running_kwh,
        quality_site_id_max_kwh,
        kwh_max_4,
        last_known_read_at,
        kwh_cume_dist,
        kwh_rank
    FROM windowed
    WHERE seq_no = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(kwh_max_4) OVER w_read_at_month_quality AS read_at_month_quality_max_kwh_max_4,
        ROUND(AVG(kwh_max_4) OVER (PARTITION BY site_id ORDER BY voltage_vs_quality_site_id_avg DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_max_4_avg_3_prior,
        ROUND(AVG(rows_in_quality_site_id) OVER (PARTITION BY MONTH(read_at), quality), 2) AS read_at_month_quality_avg_rows_in_quality_site_i,
        LEAD(voltage) OVER (PARTITION BY site_id ORDER BY voltage_vs_quality_site_id_avg DESC, id) AS next_voltage,
        quality_site_id_avg_voltage - LAG(quality_site_id_avg_voltage) OVER (PARTITION BY kwh_rank ORDER BY kwh_rank ASC, id) AS quality_site_id_avg_voltage_change
    FROM top_rows
    WINDOW w_read_at_month_quality AS (PARTITION BY MONTH(read_at), quality)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    voltage_vs_quality_site_id_avg,
    kwh_share_of_quality_site_id,
    kwh_tercile,
    quality_site_id_avg_voltage,
    quality_site_id_max_kwh,
    kwh_max_4,
    last_known_read_at,
    kwh_cume_dist,
    kwh_rank,
    read_at_month_quality_max_kwh_max_4,
    kwh_max_4_avg_3_prior,
    read_at_month_quality_avg_rows_in_quality_site_i,
    next_voltage,
    quality_site_id_avg_voltage_change
FROM ranked
WHERE read_at_month_quality_max_kwh_max_4 >= 1260.0
ORDER BY kwh_cume_dist DESC, quality_site_id_avg_voltage
