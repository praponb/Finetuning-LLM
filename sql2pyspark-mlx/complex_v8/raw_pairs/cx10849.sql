WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        ABS(voltage - 229.1) AS voltage_distance
    FROM meter_readings
    WHERE (site_id >= 7 OR read_at >= TIMESTAMP '2025-01-19 09:00:00')
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        voltage_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-18 13:00:00'
              AND m.voltage >= 218.2
        )
      AND s.quality IN ('BAD', 'OK')
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        voltage_distance,
        COALESCE(voltage_distance, 0.0) AS voltage_distance_filled,
        CASE WHEN voltage_distance >= 147.0 THEN 'HIGH' WHEN voltage_distance >= 85.8 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_distance_tier
    FROM screened
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM labeled s
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM summarized_rows s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
SELECT id, measure, value
FROM (
    SELECT id, kwh, voltage
    FROM profiled
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (kwh, voltage)
) unp
