WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        TO_DATE(read_at) AS read_at_date,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE kwh <= 109.3
),
quality_stats AS (
    -- Median kwh_minus_voltage per quality
    SELECT quality, PERCENTILE_APPROX(kwh_minus_voltage, 0.5) AS median_kwh_minus_voltage
    FROM base_readings
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_kwh_minus_voltage,
        ROUND(s.kwh_minus_voltage - g.median_kwh_minus_voltage, 2) AS kwh_minus_voltage_minus_median
    FROM base_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh_minus_voltage >= g.median_kwh_minus_voltage
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        quality,
        median_kwh_minus_voltage,
        kwh_minus_voltage_minus_median,
        COALESCE(median_kwh_minus_voltage, 0.0) AS median_kwh_minus_voltage_filled,
        GREATEST(kwh_minus_voltage, kwh) AS greatest_kwh_minus_voltage_kwh
    FROM outliers
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(kwh) OVER (PARTITION BY site_id ORDER BY kwh DESC, id) AS next_kwh,
        LAG(median_kwh_minus_voltage) OVER (PARTITION BY site_id ORDER BY kwh DESC, id) AS prev_median_kwh_minus_voltage,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_kwh
    FROM tagged
),
scored AS (
    -- Add calculated columns
    SELECT *, CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM windowed
)
SELECT
    id,
    quality,
    median_kwh_minus_voltage,
    kwh_minus_voltage_minus_median,
    prev_median_kwh_minus_voltage,
    quality_site_id_avg_kwh,
    quality_ref
FROM scored
WHERE quality_site_id_avg_kwh > 48.0
