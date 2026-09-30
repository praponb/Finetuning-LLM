WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.1, 2) AS voltage_adj,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality = 'BAD' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM valid s
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        voltage,
        quality,
        voltage_adj,
        reading_quality_count,
        reading_count,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM with_stats
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        quality,
        voltage_adj,
        greatest_voltage_kwh,
        MAX(voltage_adj) OVER (PARTITION BY quality) AS quality_max_voltage_adj,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        LEAD(greatest_voltage_kwh) OVER (ORDER BY read_at, id) AS next_greatest_voltage_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY reading_count ORDER BY greatest_voltage_kwh ASC, id), 3) AS greatest_voltage_kwh_percent_rank
    FROM labeled
),
latest AS (
    -- Keep the latest row per quality
    SELECT
        id,
        read_at,
        quality,
        voltage_adj,
        greatest_voltage_kwh,
        quality_max_voltage_adj,
        rows_in_quality,
        next_greatest_voltage_kwh,
        greatest_voltage_kwh_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY rows_in_quality DESC, id DESC) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest
    WHERE next_greatest_voltage_kwh > (SELECT AVG(next_greatest_voltage_kwh) FROM latest)
)
SELECT
    id,
    read_at,
    quality,
    voltage_adj,
    greatest_voltage_kwh,
    quality_max_voltage_adj,
    rows_in_quality,
    next_greatest_voltage_kwh,
    greatest_voltage_kwh_percent_rank,
    COALESCE(quality_max_voltage_adj, 0.0) AS quality_max_voltage_adj_filled
FROM with_activity
WHERE next_greatest_voltage_kwh >= 73.5
