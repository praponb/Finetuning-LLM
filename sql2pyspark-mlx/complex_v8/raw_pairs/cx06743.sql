WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        CASE WHEN voltage >= 234.7 THEN 'HIGH' WHEN voltage >= 222.6 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND kwh < 116.8
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM base_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality = 'BAD'
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM detail_rows s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh < 83.8
              AND m.quality IS NOT NULL
        )
),
unique_rows AS (
    -- Keep the latest row per reading_quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        reading_id,
        reading_kwh,
        reading_read_at,
        reading_quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY reading_quality, site_id ORDER BY read_at DESC, id DESC, reading_id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    reading_quality,
    reading_id,
    ARRAY_SORT(COLLECT_SET(reading_quality)) AS reading_quality_list,
    SUM(CASE WHEN reading_quality = 'EST' THEN 1 ELSE 0 END) AS n_reading_quality_est
FROM unique_rows
GROUP BY GROUPING SETS ((site_id, reading_quality, reading_id), (site_id), ())
