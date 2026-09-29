WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE site_id <= 9
),
with_parent_site AS (
    -- Attach site attributes
    SELECT s.*, si.commissioned_on AS site_commissioned_on
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY quality_or_unknown, quality ORDER BY read_at DESC) AS read_at_rank,
        NTILE(3) OVER w_quality_by_site_commissioned AS site_commissioned_on_tercile,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality_or_unknown), 2) AS voltage_vs_site_commissioned_on_month_quality_or,
        LAG(kwh) OVER (PARTITION BY quality_or_unknown, quality ORDER BY read_at DESC, id) AS prev_kwh,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY voltage_plus_kwh DESC, id) AS voltage_plus_kwh_decile,
        LAG(voltage_plus_kwh, 3) OVER (PARTITION BY quality_or_unknown, site_id ORDER BY kwh, id) AS prev3_voltage_plus_kwh,
        LAST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY quality_or_unknown, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality_or_unknown)) AS distinct_quality_in_site_commissioned_on_month_q,
        DENSE_RANK() OVER (PARTITION BY quality_or_unknown, quality ORDER BY read_at DESC, id) AS read_at_dense_rank,
        LAG(voltage) OVER w_quality_by_site_commissioned AS prev_voltage,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage_plus_kwh DESC, id) AS voltage_plus_kwh_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality_or_unknown, quality ORDER BY read_at DESC), 3) AS read_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality_or_unknown, site_id ORDER BY kwh), 3) AS kwh_percent_rank
    FROM with_parent_site
    WINDOW w_quality_by_site_commissioned AS (PARTITION BY quality ORDER BY site_commissioned_on DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        kwh - LAG(kwh) OVER (PARTITION BY quality_or_unknown, site_id ORDER BY read_at_dense_rank, id) AS kwh_change,
        voltage_plus_kwh - LAG(voltage_plus_kwh) OVER w_read_at_dense_rank_distinct AS voltage_plus_kwh_change,
        NTH_VALUE(prev_voltage, 2) OVER (PARTITION BY quality ORDER BY last_known_site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_voltage
    FROM with_trends
    WINDOW w_read_at_dense_rank_distinct AS (PARTITION BY read_at_dense_rank, distinct_quality_in_site_commissioned_on_month_q ORDER BY prev_kwh NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    quality,
    quality_or_unknown,
    site_commissioned_on,
    site_commissioned_on_tercile,
    voltage_vs_site_commissioned_on_month_quality_or,
    prev_kwh,
    voltage_plus_kwh_decile,
    prev3_voltage_plus_kwh,
    last_known_site_commissioned_on,
    read_at_dense_rank,
    prev_voltage,
    voltage_plus_kwh_rank,
    kwh_percent_rank,
    voltage_plus_kwh_change,
    second_prev_voltage
FROM ranked
