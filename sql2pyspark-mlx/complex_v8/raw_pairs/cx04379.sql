WITH scoped AS (
    -- Filter plays
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
    WHERE device <> 'tv'
      AND listener_id IS NOT NULL
),
with_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        t.title AS track_title,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date
    FROM scoped s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    listener_id,
    played_at,
    device,
    source,
    track_title,
    listener_interests,
    listener_signup_date,
    DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
FROM with_details_track
ORDER BY listener_signup_date DESC NULLS FIRST
