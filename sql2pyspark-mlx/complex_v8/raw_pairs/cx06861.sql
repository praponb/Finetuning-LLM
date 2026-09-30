WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
),
combined_details AS (
    -- Join tracks
    SELECT
        s.*,
        t.title AS track_title,
        t.moods AS track_moods,
        t.bpm AS track_bpm
    FROM cleaned_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM combined_details s
),
per_label AS (
    -- One row per track_mood
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.track_title,
        s.track_bpm,
        s.last_played_at,
        t.track_mood
    FROM with_counts s
    LATERAL VIEW OUTER EXPLODE(s.track_moods) t AS track_mood
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', last_played_at) AS days_since_last_played_at
    FROM per_label
),
enriched AS (
    -- Attach track attributes
    SELECT s.*, t.artist_id AS track_artist_id
    FROM annotated s
    JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    track_bpm,
    last_played_at,
    track_mood,
    days_since_last_played_at,
    track_artist_id,
    ROUND(ms_played / NULLIF(track_bpm, 0), 3) AS ms_played_per_track_bpm,
    CASE WHEN track_mood IN ('chill', 'party') THEN 'group_a' WHEN track_mood = 'focus' THEN 'group_b' ELSE 'other' END AS track_mood_group
FROM enriched
ORDER BY track_artist_id
