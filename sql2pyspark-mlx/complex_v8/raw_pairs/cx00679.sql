WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(voltage * 0.9, 2) AS voltage_adj,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR kwh <= 92.5)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY days_since_read_at), 3) AS days_since_read_at_cume_dist,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)), 2) AS kwh_vs_read_at_month_avg,
        FIRST_VALUE(voltage_adj) OVER (PARTITION BY site_id ORDER BY days_since_read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage_adj,
        MAX(days_since_read_at) OVER (PARTITION BY site_id ORDER BY days_since_read_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_read_at_max_7,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY read_at, id) AS read_at_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY days_since_read_at DESC), 3) AS days_since_read_at_cume_dist_2,
        LEAD(read_at, 3) OVER (PARTITION BY site_id ORDER BY days_since_read_at, id) AS next3_read_at,
        ROUND(AVG(days_since_read_at) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_read_at_avg_1_prior,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at))) AS distinct_site_id_in_read_at_month
    FROM recent_readings
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(next3_read_at, 'MMM yyyy') AS next3_read_at_label,
        COALESCE(read_at_decile, 0) AS read_at_decile_filled,
        LEAST(first_known_kwh, voltage_adj) AS least_first_known_kwh_voltage_adj
    FROM with_windows
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage_adj,
    days_since_read_at,
    days_since_read_at_cume_dist,
    first_known_kwh,
    kwh_vs_read_at_month_avg,
    first_voltage_adj,
    read_at_decile,
    days_since_read_at_cume_dist_2,
    days_since_read_at_avg_1_prior,
    distinct_site_id_in_read_at_month,
    read_at_decile_filled,
    INITCAP(TRIM(next3_read_at_label)) AS next3_read_at_label_clean,
    ROUND(rows_in_site_id / NULLIF(kwh_vs_read_at_month_avg, 0), 3) AS rows_in_site_id_per_kwh_vs_read_at_month_avg
FROM tagged
ORDER BY read_at_decile NULLS LAST
