WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE kwh >= 26.9
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_month,
        voltage_per_kwh,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage_per_kwh, id) AS rn,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_quality_read_at_month_to_dat, 0), 4) AS kwh_share_of_quality_read_at_month_to_date,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, TO_DATE(read_at_month)), 2) AS quality_read_at_month_to_date_avg_voltage,
        SIZE(COLLECT_SET(site_id) OVER w_quality_read_at_month_to_dat) AS distinct_site_id_in_quality_read_at_month_to_dat,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at_month DESC, id), 3) AS read_at_month_cume_dist,
        LEAD(voltage_per_kwh) OVER (PARTITION BY quality, site_id ORDER BY voltage_per_kwh, id) AS next_voltage_per_kwh,
        ROUND(voltage_per_kwh / NULLIF(SUM(voltage_per_kwh) OVER w_quality_read_at_month_to_dat, 0), 4) AS voltage_per_kwh_share_of_quality_read_at_month_t,
        voltage_per_kwh - LAG(voltage_per_kwh) OVER (PARTITION BY quality ORDER BY read_at DESC, id) AS voltage_per_kwh_change,
        LAST_VALUE(read_at_month) OVER (PARTITION BY quality, site_id ORDER BY voltage_per_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_month
    FROM filtered_readings
    WINDOW w_quality_read_at_month_to_dat AS (PARTITION BY quality, TO_DATE(read_at_month))
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(quality, '#', CAST(rn AS STRING)) AS quality_ref,
        GREATEST(voltage_per_kwh_change, next_voltage_per_kwh) AS greatest_voltage_per_kwh_change_next_voltage_per,
        ABS(read_at_month_cume_dist - 0.7) AS read_at_month_cume_dist_distance
    FROM sequenced
    WHERE quality_read_at_month_to_date_avg_voltage <= 231.0
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    read_at_month,
    voltage_per_kwh,
    kwh_share_of_quality_read_at_month_to_date,
    distinct_site_id_in_quality_read_at_month_to_dat,
    voltage_per_kwh_change,
    HOUR(read_at_month) AS read_at_month_hour
FROM annotated
ORDER BY voltage DESC NULLS FIRST
