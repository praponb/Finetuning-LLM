WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE artist_id IS NOT NULL
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.release_date_week,
        s.title_slug,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.device AS play_device,
        p.played_at AS play_played_at
    FROM with_activity s
    LEFT JOIN plays p ON p.track_id = s.id
),
latest_per_key AS (
    -- Keep the earliest row per play_device, play_listener_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        release_date_week,
        title_slug,
        play_id,
        play_listener_id,
        play_device,
        play_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_device, play_listener_id ORDER BY play_played_at, id, play_id) AS rn
        FROM detail_rows
    ) ranked
    WHERE rn = 1
),
with_activity_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source IS NOT NULL
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        MIN(release_date_week) AS first_release_date_week,
        ROUND(SUM(bpm) / NULLIF(SUM(duration_sec), 0), 3) AS bpm_to_duration_sec_ratio,
        COUNT(*) FILTER (WHERE play_device IN ('phone', 'tv', 'speaker')) AS n_play_device_in_phone_tv_spea,
        MIN(play_played_at) AS first_play_played_at,
        ROUND(SUM(duration_sec) / NULLIF(SUM(bpm), 0), 3) AS duration_sec_to_bpm_ratio
    FROM with_activity_2
    WHERE HOUR(release_date_week) BETWEEN 6 AND 12
    GROUP BY 1
)
SELECT
    artist_id,
    bpm_to_duration_sec_ratio,
    duration_sec_to_bpm_ratio,
    DATE_TRUNC('HOUR', first_play_played_at) AS first_play_played_at_hour
FROM stats
ORDER BY duration_sec_to_bpm_ratio DESC
