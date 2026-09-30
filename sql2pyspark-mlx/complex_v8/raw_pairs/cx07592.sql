WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE device = 'phone'
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        QUARTER(played_at) AS played_at_quarter,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM base_plays
    WHERE track_id IS NOT NULL
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.is_skipped_label,
        s.days_since_played_at,
        s.played_at_label,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM annotated s
    JOIN plays p ON p.track_id = s.track_id
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM exploded_detail_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 13
        )
),
latest AS (
    -- Keep the latest row per play_listener_id
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        is_skipped_label,
        days_since_played_at,
        played_at_label,
        play_id,
        play_ms_played,
        play_played_at,
        play_device,
        play_listener_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_listener_id ORDER BY play_played_at DESC, id DESC, play_id DESC) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MAX(played_at) AS last_played_at,
        SUM(ms_played) AS total_play_ms_played,
        SUM(ms_played) AS total_play_ms_played_2
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.total_play_ms_played,
        p.total_play_ms_played_2
    FROM latest s
    LEFT JOIN play_stats p ON p.listener_id = s.play_listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    is_skipped_label,
    days_since_played_at,
    played_at_label,
    play_id,
    play_ms_played,
    play_played_at,
    play_device,
    play_listener_id,
    last_played_at,
    total_play_ms_played,
    total_play_ms_played_2
FROM with_stats
