WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage DESC, id DESC) AS rn
        FROM valid_readings
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 216.5 AND 234.5
              AND MONTH(m.read_at) IN (1, 2)
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio
FROM without_matches
GROUP BY GROUPING SETS ((quality, site_id), (quality))
ORDER BY quality NULLS LAST, site_id NULLS LAST
