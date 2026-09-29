WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE site_id >= 6
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(kwh, 1, 0) OVER w_all_by_kwh AS next_kwh,
        RANK() OVER (ORDER BY kwh DESC, id) AS kwh_rank,
        ROUND(CUME_DIST() OVER (ORDER BY kwh), 3) AS kwh_cume_dist,
        LEAD(kwh_per_voltage, 3, 0) OVER w_all_by_kwh AS next3_kwh_per_voltage,
        SUM(voltage_bucket) OVER (ORDER BY kwh_minus_voltage DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_bucket_sum_2_prior,
        ROUND(CUME_DIST() OVER (ORDER BY kwh DESC, id), 3) AS kwh_cume_dist_2
    FROM scoped
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        kwh,
        quality,
        kwh_per_voltage,
        voltage_bucket,
        kwh_minus_voltage,
        next_kwh,
        kwh_rank,
        kwh_cume_dist,
        voltage_bucket_sum_2_prior,
        read_at + INTERVAL 15 MINUTES AS read_at_plus_15m,
        YEAR(read_at) AS read_at_year
    FROM metrics
    WHERE site_id IS NOT NULL
)
SELECT
    kwh_rank,
    read_at_year,
    SUM(CASE WHEN quality = 'OK' THEN kwh_per_voltage ELSE 0 END) AS kwh_per_voltage_quality_ok
FROM derived
GROUP BY CUBE(kwh_rank, read_at_year)
ORDER BY kwh_rank NULLS LAST, read_at_year NULLS LAST
