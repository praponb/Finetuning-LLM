WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE voltage >= 218.2
      AND read_at < TIMESTAMP '2025-03-02 11:00:00'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        least_kwh_voltage,
        LAST_VALUE(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_hour) ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        ROUND((UNIX_TIMESTAMP(read_at_hour) - UNIX_TIMESTAMP(LAG(read_at_hour) OVER w_read_at_hour_month_by_voltag_2)) / 60, 1) AS minutes_since_prev_read_at_hour,
        NTILE(10) OVER (ORDER BY least_kwh_voltage DESC, id) AS least_kwh_voltage_decile,
        DENSE_RANK() OVER w_read_at_hour_month_by_voltag_2 AS voltage_dense_rank,
        LAG(read_at_hour, 2) OVER (PARTITION BY quality ORDER BY read_at_hour DESC NULLS FIRST, id) AS prev2_read_at_hour,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        NTILE(10) OVER (ORDER BY read_at_hour DESC, id) AS read_at_hour_decile,
        COUNT(*) OVER (PARTITION BY YEAR(read_at_hour), quality) AS rows_in_read_at_hour_year_quality,
        DENSE_RANK() OVER (ORDER BY least_kwh_voltage DESC) AS least_kwh_voltage_dense_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY YEAR(read_at_hour), quality), 0), 4) AS kwh_share_of_read_at_hour_year_quality,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_hour) ORDER BY voltage) AS voltage_rank
    FROM recent_readings
    WINDOW w_read_at_hour_month_by_voltag_2 AS (PARTITION BY DATE_TRUNC('MONTH', read_at_hour) ORDER BY voltage, id)
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    least_kwh_voltage,
    last_kwh,
    least_kwh_voltage_decile,
    voltage_dense_rank,
    prev2_read_at_hour,
    rows_in_site_id,
    read_at_hour_decile,
    rows_in_read_at_hour_year_quality,
    least_kwh_voltage_dense_rank,
    voltage_share_of_site_id,
    voltage_rank
FROM with_trends
ORDER BY read_at_hour_decile NULLS LAST, kwh NULLS LAST
