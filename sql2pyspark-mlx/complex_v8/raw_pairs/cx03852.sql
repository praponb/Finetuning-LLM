WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE ms_played > 71476
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source_prefix,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date,
        t.bpm AS track_bpm
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source_prefix,
        s.listener_signup_date,
        s.track_bpm,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods,
        t.bpm AS track_bpm_2,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM joined s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source_prefix,
    track_bpm,
    track_duration_sec,
    track_moods,
    track_bpm_2,
    listener_birth_year,
    LOWER(REGEXP_REPLACE(TRIM(source_prefix), ' ', '_')) AS source_prefix_slug
FROM with_parent
ORDER BY id, track_duration_sec
