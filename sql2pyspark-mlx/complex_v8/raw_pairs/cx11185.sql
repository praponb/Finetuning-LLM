WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE ms_played >= 112786
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.days_since_played_at,
        l.username AS listener_username,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM recent_plays s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.birth_year <= 1990
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        CONCAT(listener_username, '#', CAST(listener_id AS STRING)) AS listener_username_ref
    FROM combined_details
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.days_since_played_at,
        s.listener_interests,
        s.listener_birth_year,
        s.ms_played_bucket,
        t.is_explicit AS track_is_explicit,
        t.artist_id AS track_artist_id
    FROM derived s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.artist_id IS NOT NULL
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.bpm AS track_bpm, l.plan AS listener_plan
    FROM enriched s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
    WHERE t.bpm > 76
)
SELECT *
FROM (
    SELECT track_is_explicit, track_id, listener_plan, ms_played_bucket
    FROM with_details_track
) src
PIVOT (
    SUM(ms_played_bucket) AS total, MAX(ms_played_bucket) AS max_v
    FOR listener_plan IN ('student', 'free', 'family', 'premium')
)
ORDER BY track_is_explicit, track_id
