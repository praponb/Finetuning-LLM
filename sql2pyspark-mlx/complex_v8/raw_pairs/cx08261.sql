WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE site_id >= 2
      AND voltage BETWEEN 213.5 AND 234.7
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
qualified_2 AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-24 14:00:00'
              AND m.voltage <= 231.7
        )
),
deduped AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        least_kwh_voltage,
        quality_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh DESC, id DESC) AS row_num
        FROM qualified_2
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    quality_or_unknown,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM deduped
GROUP BY GROUPING SETS ((site_id, quality_or_unknown), (site_id), (quality_or_unknown), ())
ORDER BY site_id NULLS LAST, quality_or_unknown NULLS LAST
