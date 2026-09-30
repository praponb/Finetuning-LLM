WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE device IN ('speaker', 'car')
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM recent s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
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
        s.ms_played_bucket,
        s.device_len,
        s.play_count,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year,
        t.moods AS track_moods,
        t.bpm AS track_bpm
    FROM with_stats s
    JOIN listeners l ON l.id = s.listener_id AND LENGTH(TRIM(l.username)) >= 6
    LEFT JOIN tracks t ON t.id = s.track_id
),
combined_details_2 AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_bucket,
        s.device_len,
        s.listener_country,
        s.listener_birth_year,
        s.track_moods,
        s.track_bpm,
        t.moods AS track_moods_2
    FROM combined_details s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.device_len,
        s.listener_country,
        s.listener_birth_year,
        s.track_moods,
        s.track_bpm,
        s.track_moods_2,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date
    FROM combined_details_2 s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        l.plan AS listener_plan_2,
        l.username AS listener_username
    FROM with_details s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.listener_country,
        s.listener_birth_year,
        s.listener_signup_date,
        s.track_is_explicit,
        s.track_release_date,
        s.track_duration_sec,
        s.track_title,
        s.listener_plan_2,
        s.listener_username,
        l.interests AS listener_interests,
        l.country AS listener_country_2,
        l.signup_date AS listener_signup_date_2,
        t.release_date AS track_release_date_2
    FROM joined_track s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    source,
    listener_country,
    listener_birth_year,
    listener_signup_date,
    track_is_explicit,
    track_duration_sec,
    track_title,
    listener_plan_2,
    listener_username,
    listener_country_2,
    listener_signup_date_2,
    track_release_date_2,
    DATE_TRUNC('YEAR', track_release_date_2) AS track_release_date_2_year,
    COALESCE(listener_country_2, 'UNKNOWN') AS listener_country_2_or_unknown
FROM with_parent
ORDER BY listener_signup_date DESC, track_title DESC
