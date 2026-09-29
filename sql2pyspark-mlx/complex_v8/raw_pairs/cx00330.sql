WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage) OVER w_site_id, 2) AS site_id_avg_voltage,
        DENSE_RANK() OVER (ORDER BY voltage DESC NULLS FIRST, id) AS voltage_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage), 3) AS voltage_percent_rank,
        kwh - LAG(kwh) OVER (ORDER BY voltage DESC NULLS FIRST, id) AS kwh_change,
        LEAD(kwh, 3, 0) OVER (ORDER BY voltage DESC NULLS FIRST, id) AS next3_kwh,
        DENSE_RANK() OVER (ORDER BY voltage, id) AS voltage_dense_rank_2,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id, YEAR(read_at)), 0), 4) AS kwh_share_of_site_id_read_at_year,
        RANK() OVER (ORDER BY voltage DESC NULLS FIRST, id) AS voltage_rank,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY voltage), 3) AS voltage_cume_dist,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh
    FROM scoped_readings
    WINDOW w_site_id AS (PARTITION BY site_id)
),
enriched AS (
    -- Label and score rows
    SELECT *, read_at + INTERVAL 7 DAYS AS read_at_plus_7d
    FROM metrics
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        second_voltage - LAG(second_voltage) OVER w_read_at_plus_7d_month_site_i AS second_voltage_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage_dense_rank), 3) AS voltage_dense_rank_percent_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY site_id_max_kwh ASC NULLS LAST, id) AS site_id_max_kwh_rank,
        ROUND(AVG(kwh) OVER (ORDER BY voltage_dense_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        ROW_NUMBER() OVER w_read_at_month_by_site_id_max AS site_id_max_kwh_seq
    FROM enriched
    WINDOW w_read_at_plus_7d_month_site_i AS (PARTITION BY MONTH(read_at_plus_7d), site_id ORDER BY kwh_change DESC, id), w_read_at_month_by_site_id_max AS (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY site_id_max_kwh ASC NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    site_id_avg_voltage,
    voltage_dense_rank,
    next3_kwh,
    rows_in_site_id,
    kwh_share_of_site_id_read_at_year,
    voltage_rank,
    second_voltage,
    site_id_max_kwh,
    second_voltage_change,
    voltage_dense_rank_percent_rank,
    site_id_max_kwh_rank,
    GREATEST(voltage_dense_rank, second_voltage) AS greatest_voltage_dense_rank_second_voltage
FROM metrics_2
ORDER BY site_id_avg_voltage DESC, voltage
