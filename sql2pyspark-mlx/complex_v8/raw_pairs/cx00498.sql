WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE is_skipped = FALSE
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = TRUE
        )
)
SELECT
    id,
    track_id,
    ms_played,
    device,
    device_slug,
    played_at_year,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label,
    played_at_year + INTERVAL 2 HOURS AS played_at_year_plus_2h
FROM screened
WHERE device IN ('speaker', 'desktop', 'car')
ORDER BY ms_played DESC
