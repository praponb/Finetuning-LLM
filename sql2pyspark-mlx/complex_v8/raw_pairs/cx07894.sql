WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE site_id >= 3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(read_at, 2) OVER (ORDER BY kwh_minus_voltage DESC, id) AS prev2_read_at,
        MIN(kwh_minus_voltage) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_minus_voltage_so_far,
        LEAD(voltage) OVER w_site_id_by_read_at AS next_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY voltage, id), 3) AS voltage_percent_rank,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        greatest_kwh_voltage - LAG(greatest_kwh_voltage) OVER (PARTITION BY quality ORDER BY voltage, id) AS greatest_kwh_voltage_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_percent_rank,
        RANK() OVER (ORDER BY kwh_minus_voltage DESC, id) AS kwh_minus_voltage_rank,
        COUNT(*) OVER w_quality AS rows_in_quality
    FROM recent_readings
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at, id), w_quality AS (PARTITION BY quality)
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_known_read_at) AS first_known_read_at_month,
        quality,
        CASE WHEN next_voltage >= 233.1 THEN 'large' WHEN next_voltage >= 220.2 THEN 'medium' ELSE 'small' END AS next_voltage_tier,
        ROUND(AVG(rows_in_quality), 2) AS avg_rows_in_quality,
        COUNT(*) FILTER (WHERE quality = 'EST') AS n_quality_est,
        COUNT(*) AS n_rows,
        MIN(prev2_read_at) AS first_prev2_read_at
    FROM ranked
    WHERE site_id IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', first_known_read_at), quality, CASE WHEN next_voltage >= 233.1 THEN 'large' WHEN next_voltage >= 220.2 THEN 'medium' ELSE 'small' END
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(n_quality_est) OVER (PARTITION BY n_rows, n_quality_est ORDER BY n_rows NULLS LAST, first_known_read_at_month, quality, next_voltage_tier) AS prev_n_quality_est,
        ROUND(AVG(n_rows) OVER (PARTITION BY n_rows, n_quality_est ORDER BY n_rows NULLS LAST, first_known_read_at_month, quality, next_voltage_tier ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS n_rows_avg_6_prior
    FROM totals
)
-- Final projection
SELECT
    first_known_read_at_month,
    next_voltage_tier,
    avg_rows_in_quality,
    n_rows,
    first_prev2_read_at,
    prev_n_quality_est
FROM windowed
