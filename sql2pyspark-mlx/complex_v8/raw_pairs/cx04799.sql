WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.1, 2) AS voltage_adj
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-02'
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_adj,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM prepared s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality IN ('BAD', 'OK')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(kwh) OVER w_reading_quality_reading_read AS reading_quality_reading_read_at_month_min_kwh,
        LEAD(reading_read_at) OVER w_reading_read_at_month_by_rea AS next_reading_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY reading_quality, MONTH(reading_read_at)), 2) AS kwh_vs_reading_quality_reading_read_at_month_avg,
        ROUND((UNIX_TIMESTAMP(reading_read_at) - UNIX_TIMESTAMP(LAG(reading_read_at) OVER w_reading_read_at_month_by_rea)) / 60, 1) AS minutes_since_prev_reading_read_at
    FROM with_children_readings
    WINDOW w_reading_quality_reading_read AS (PARTITION BY reading_quality, MONTH(reading_read_at)), w_reading_read_at_month_by_rea AS (PARTITION BY MONTH(reading_read_at) ORDER BY reading_read_at ASC, id, reading_id)
)
SELECT
    quality,
    reading_quality,
    MIN(reading_quality_reading_read_at_month_min_kwh) AS min_reading_quality_reading_read_at_month_min_kw,
    SUM(CASE WHEN reading_quality IN ('EST', 'BAD') THEN 1 ELSE 0 END) AS n_reading_quality_in_est_bad
FROM sequenced
GROUP BY GROUPING SETS ((quality, reading_quality), (quality), ())
ORDER BY quality NULLS LAST, reading_quality NULLS LAST
