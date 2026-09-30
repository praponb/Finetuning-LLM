WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
),
site_id_stats AS (
    -- Max voltage per site_id
    SELECT site_id, MAX(voltage) AS max_voltage
    FROM cleaned
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM cleaned s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage = g.max_voltage
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(max_voltage) OVER (ORDER BY max_voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_max_voltage_so_far,
        SUM(kwh) OVER (ORDER BY max_voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality
    FROM above_average
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(quality_prefix)) AS quality_prefix_len,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM sequenced
)
SELECT *
FROM (
    SELECT distinct_site_id_in_quality, site_id, quality, voltage_minus_max
    FROM derived
) src
PIVOT (
    COUNT(voltage_minus_max)
    FOR quality IN ('BAD', 'OK')
)
