WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.5, 2) AS kwh_adj
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage,
        COUNT(*) AS reading_count,
        MAX(kwh) AS max_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.max_reading_kwh
    FROM recent s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        avg_reading_voltage,
        max_reading_kwh,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, reading_count)) AS distinct_site_id_in_quality_reading_count,
        voltage - LAG(voltage) OVER (PARTITION BY MONTH(read_at), reading_count ORDER BY read_at, id) AS voltage_change
    FROM with_stats
),
derived AS (
    -- Derive helper columns
    SELECT *, ABS(voltage - 220.7) AS voltage_distance
    FROM with_windows
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM derived s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(voltage_change) OVER (PARTITION BY site_id ORDER BY voltage_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_change_avg_2_prior,
        LEAD(max_reading_kwh, 2) OVER (PARTITION BY distinct_site_id_in_quality_reading_count ORDER BY kwh DESC, id) AS next2_max_reading_kwh
    FROM summarized_rows
),
derived_2 AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM windowed
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at_month, distinct_site_id_in_quality_reading_count, quality, voltage_change
    FROM derived_2
) src
PIVOT (
    MAX(voltage_change) AS max_v, COUNT(voltage_change) AS n
    FOR quality IN ('EST', 'BAD')
)
