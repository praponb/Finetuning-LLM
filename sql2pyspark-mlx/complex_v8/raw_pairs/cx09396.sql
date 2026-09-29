WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        ABS(ms_played - 130888) AS ms_played_distance,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE listener_id IS NOT NULL
      AND track_id IS NOT NULL
),
stats AS (
    -- Group-level totals
    SELECT
        track_id,
        source,
        listener_id,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS row_count,
        MAX(ms_played_bucket) AS max_ms_played_bucket
    FROM recent
    GROUP BY track_id, source, listener_id
),
joined AS (
    -- Attach track attributes
    SELECT
        s.track_id,
        s.source,
        s.listener_id,
        s.last_played_at,
        s.row_count,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title
    FROM stats s
    LEFT JOIN tracks t ON t.id = s.track_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(row_count - 7) AS row_count_distance,
        FLOOR(track_bpm / 25) * 25 AS track_bpm_bucket
    FROM joined
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.track_id,
        s.source,
        s.listener_id,
        s.last_played_at,
        s.row_count,
        s.track_is_explicit,
        s.track_title,
        s.row_count_distance,
        s.track_bpm_bucket,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM enriched s
    JOIN plays p ON p.listener_id = s.listener_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.track_id,
        s.source,
        s.listener_id,
        s.track_is_explicit,
        s.row_count_distance,
        s.play_id,
        s.play_played_at,
        s.play_source,
        p.id AS play_id_2,
        p.is_skipped AS play_is_skipped_2,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at_2
    FROM exploded_detail_plays s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT
    track_id,
    source,
    listener_id,
    track_is_explicit,
    row_count_distance,
    play_id,
    play_played_at,
    play_source,
    play_id_2,
    play_is_skipped_2,
    ROUND(row_count_distance / NULLIF(play_ms_played, 0), 3) AS row_count_distance_per_play_ms_played,
    CONCAT_WS(' / ', source, play_source) AS source_play_source_label
FROM exploded_detail
