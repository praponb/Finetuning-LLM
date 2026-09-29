WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date,
        CONCAT(device, '#', CAST(track_id AS STRING)) AS device_ref,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m
    FROM plays
    WHERE track_id IS NOT NULL
),
joined AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.device_ref,
        l.interests AS listener_interests,
        l.country AS listener_country,
        t.bpm AS track_bpm,
        t.release_date AS track_release_date
    FROM base s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE l.country IN ('BR', 'TH')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(track_bpm) OVER (PARTITION BY source, is_skipped) AS source_is_skipped_sum_track_bpm,
        COUNT(*) OVER (PARTITION BY source, is_skipped) AS rows_in_source_is_skipped,
        LAG(track_bpm) OVER (ORDER BY played_at ASC, id) AS prev_track_bpm,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source, is_skipped), 2) AS ms_played_vs_source_is_skipped_avg
    FROM joined
),
joined_2 AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.device_ref,
        s.listener_interests,
        s.listener_country,
        s.track_bpm,
        s.track_release_date,
        s.source_is_skipped_sum_track_bpm,
        s.rows_in_source_is_skipped,
        s.prev_track_bpm,
        l.plan AS listener_plan
    FROM windowed s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    device_ref,
    listener_interests,
    listener_country,
    track_bpm,
    track_release_date,
    source_is_skipped_sum_track_bpm,
    prev_track_bpm,
    listener_plan,
    YEAR(track_release_date) AS track_release_date_year,
    prev_track_bpm - track_bpm AS prev_track_bpm_minus_track_bpm
FROM joined_2
WHERE source IN ('search', 'album', 'radio')
ORDER BY prev_track_bpm DESC, listener_id
