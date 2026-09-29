WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        QUARTER(played_at) AS played_at_quarter
    FROM plays
    WHERE listener_id <= 16
      AND HOUR(played_at) BETWEEN 6 AND 11
),
screened AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.birth_year BETWEEN 1968 AND 1994
              AND l.signup_date >= DATE '2024-07-13'
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(ms_played, played_at_quarter) AS least_ms_played_played_at_quarter,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref
    FROM screened
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    is_skipped,
    played_at_quarter,
    least_ms_played_played_at_quarter,
    device_ref,
    ABS(played_at_quarter - 2) AS played_at_quarter_distance,
    LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
FROM enriched
WHERE TO_DATE(played_at) < DATE '2025-03-08'
ORDER BY device_ref, played_at_quarter NULLS LAST, id
LIMIT 5
