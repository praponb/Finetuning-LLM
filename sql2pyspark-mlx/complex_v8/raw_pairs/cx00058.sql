WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
    WHERE quality = 'BAD'
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 113.1
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(voltage) OVER (PARTITION BY read_at_hour) AS read_at_hour_min_voltage,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY read_at_hour)) AS distinct_site_id_in_read_at_hour,
        NTILE(3) OVER w_read_at_hour_by_read_at_hour AS read_at_hour_tercile
    FROM without_matches
    WINDOW w_read_at_hour_by_read_at_hour AS (PARTITION BY read_at_hour ORDER BY read_at_hour, id)
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_slug,
        read_at_hour,
        read_at_hour_min_voltage,
        distinct_site_id_in_read_at_hour,
        read_at_hour_tercile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at_hour_min_voltage DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
without_matches_2 AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-12 14:00:00'
              AND m.quality = 'EST'
        )
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
        quality_slug,
        read_at_hour,
        read_at_hour_min_voltage,
        distinct_site_id_in_read_at_hour,
        read_at_hour_tercile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM without_matches_2
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    quality,
    read_at_hour_min_voltage,
    distinct_site_id_in_read_at_hour,
    read_at_hour_tercile,
    LOWER(TRIM(quality_slug)) AS quality_slug_clean
FROM latest
WHERE read_at_hour_tercile <= 9
ORDER BY voltage DESC
