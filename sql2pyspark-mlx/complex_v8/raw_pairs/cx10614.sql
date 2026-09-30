WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
      AND track_id IS NOT NULL
),
joined_listener AS (
    -- Join listeners
    SELECT
        s.*,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date
    FROM valid_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM joined_listener s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device = 'speaker'
        )
      AND s.played_at >= TIMESTAMP '2025-01-19 13:00:00'
)
-- Final projection
SELECT
    id,
    played_at,
    is_skipped,
    source,
    listener_interests,
    listener_signup_date,
    YEAR(listener_signup_date) AS listener_signup_date_year
FROM screened
WHERE ARRAY_CONTAINS(listener_interests, 'live')
ORDER BY listener_signup_date_year DESC NULLS FIRST, source DESC NULLS FIRST
