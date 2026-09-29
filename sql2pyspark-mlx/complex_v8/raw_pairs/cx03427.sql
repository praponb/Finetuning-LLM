WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY voltage ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        RANK() OVER w_all_by_voltage_ties AS voltage_rank,
        LAST_VALUE(read_at) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        SUM(voltage) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg
    FROM recent_readings
    WINDOW w_site_id AS (PARTITION BY site_id), w_all_by_voltage_ties AS (ORDER BY voltage ASC)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(voltage_rank, 2) OVER (PARTITION BY site_id ORDER BY voltage_vs_site_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_rank,
        voltage - LAG(voltage) OVER w_last_read_at_month_site_id_b AS voltage_change,
        NTH_VALUE(first_kwh, 2) OVER (PARTITION BY site_id ORDER BY voltage_vs_site_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_kwh,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_read_at), site_id ORDER BY running_voltage DESC) AS running_voltage_dense_rank,
        LAST_VALUE(first_kwh) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', last_read_at), site_id ORDER BY running_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_kwh,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', last_read_at), site_id ORDER BY running_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(kwh_vs_site_id_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', last_read_at), site_id ORDER BY running_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh_vs_site_id_avg,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id
    FROM metrics
    WINDOW w_last_read_at_month_site_id_b AS (PARTITION BY DATE_TRUNC('MONTH', last_read_at), site_id ORDER BY running_voltage DESC, id)
)
SELECT
    id,
    read_at,
    kwh,
    voltage,
    minutes_since_prev_read_at,
    site_id_min_kwh,
    kwh_vs_site_id_avg,
    voltage_rank,
    last_read_at,
    voltage_share_of_site_id,
    voltage_vs_site_id_avg,
    second_first_kwh,
    running_voltage_dense_rank,
    last_known_first_kwh,
    last_kwh_vs_site_id_avg,
    rows_in_site_id,
    ROUND(last_known_first_kwh / NULLIF(first_kwh, 0), 3) AS last_known_first_kwh_per_first_kwh
FROM ranked
ORDER BY site_id_min_kwh DESC
