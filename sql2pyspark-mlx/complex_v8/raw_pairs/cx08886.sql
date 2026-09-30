WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group_2
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 10:00:00'
      AND quality = 'OK'
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM base
    WHERE kwh > (SELECT MIN(kwh) FROM base)
),
quality_stats AS (
    -- Avg voltage per quality
    SELECT quality, AVG(voltage) AS avg_voltage
    FROM filtered
    GROUP BY quality
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM filtered s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage > g.avg_voltage
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_group,
        s.quality_group_2,
        s.avg_voltage,
        s.voltage_minus_avg,
        si.tariff AS site_tariff
    FROM vs_group s
    JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM joined s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_group,
    quality_group_2,
    avg_voltage,
    avg_voltage + voltage_minus_avg AS avg_voltage_plus_voltage_minus_avg
FROM summarized_rows
ORDER BY id DESC, read_at
