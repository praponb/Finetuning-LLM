WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE listener_id >= 4
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM valid_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, t.moods AS track_moods, t.bpm AS track_bpm
    FROM with_counts s
    JOIN tracks t ON t.id = s.track_id
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.*,
        t.bpm AS track_bpm_2,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title
    FROM joined s
    JOIN tracks t ON t.id = s.track_id AND t.is_explicit = TRUE
),
latest AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        last_played_at,
        track_moods,
        track_bpm,
        track_bpm_2,
        track_duration_sec,
        track_title
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY last_played_at DESC, id DESC) AS rn
        FROM combined_details_track
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device, listener_id)) AS distinct_track_id_in_device_listener_id,
        NTILE(4) OVER (PARTITION BY source, listener_id ORDER BY ms_played, id) AS ms_played_quartile
    FROM latest
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = TRUE
              AND HOUR(p.played_at) BETWEEN 6 AND 12
        )
)
SELECT
    id,
    listener_id,
    ms_played,
    device,
    last_played_at,
    track_moods,
    track_bpm,
    track_bpm_2,
    track_duration_sec,
    track_title,
    distinct_track_id_in_device_listener_id,
    ms_played_quartile
FROM screened
ORDER BY device
