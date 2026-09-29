WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE site_id >= 4
),
rollup_base AS (
    -- Group-level totals
    SELECT
        site_id,
        quality,
        MAX(voltage) AS max_voltage,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT quality) AS distinct_qualitys
    FROM base
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM rollup_base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 236.7
              AND m.quality = 'OK'
        )
)
-- Final projection
SELECT
    site_id,
    quality,
    max_voltage,
    n_rows,
    distinct_ids,
    distinct_qualitys,
    CASE WHEN distinct_qualitys >= 7 THEN 'large' WHEN distinct_qualitys >= 3 THEN 'medium' ELSE 'small' END AS distinct_qualitys_tier
FROM with_activity
WHERE quality <> 'OK'
ORDER BY max_voltage DESC, quality DESC NULLS FIRST
