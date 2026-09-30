WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4,
        COUNT(*) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)) AS rows_in_site_id_read_at_month,
        ROUND(CUME_DIST() OVER w_site_id_by_read_at, 3) AS read_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY kwh, id), 3) AS kwh_percent_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST) AS voltage_dense_rank,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        ROUND(AVG(kwh) OVER w_site_id_read_at_month, 2) AS site_id_read_at_month_avg_kwh,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY kwh, id) AS voltage_change,
        SUM(voltage) OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        LEAD(read_at) OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id) AS next_read_at,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        SUM(voltage) OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_sum_4,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_read_at
    FROM scoped
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at, id), w_site_id_read_at_month AS (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at))
)
SELECT
    id,
    site_id,
    read_at,
    voltage_plus_kwh,
    kwh_max_4,
    rows_in_site_id_read_at_month,
    read_at_cume_dist,
    voltage_dense_rank,
    site_id_read_at_month_avg_kwh,
    voltage_change,
    running_voltage,
    first_kwh,
    voltage_sum_4,
    ABS(read_at_cume_dist - 0.3) AS read_at_cume_dist_distance
FROM with_windows
WHERE kwh_percent_rank >= 0.1
ORDER BY read_at DESC, id
LIMIT 10
