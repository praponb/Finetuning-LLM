WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        COALESCE(ms_played, 0) AS ms_played_filled,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE source = 'search'
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM recent s
    JOIN plays p ON p.listener_id = s.listener_id AND p.played_at >= TIMESTAMP '2025-01-13 12:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY play_played_at DESC NULLS FIRST), 3) AS play_played_at_cume_dist,
        NTILE(4) OVER (PARTITION BY play_is_skipped ORDER BY play_played_at ASC, id, play_id) AS play_played_at_quartile,
        NTILE(3) OVER w_play_is_skipped_by_play_play AS play_played_at_tercile,
        LEAD(play_played_at) OVER (PARTITION BY play_is_skipped ORDER BY play_played_at ASC, id, play_id) AS next_play_played_at,
        RANK() OVER (PARTITION BY play_is_skipped ORDER BY play_played_at ASC) AS play_played_at_rank
    FROM exploded_detail
    WINDOW w_play_is_skipped_by_play_play AS (PARTITION BY play_is_skipped ORDER BY play_played_at ASC, id, play_id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        device,
        is_skipped,
        play_id,
        play_device,
        play_is_skipped,
        play_played_at_cume_dist,
        play_played_at_quartile,
        play_played_at_tercile,
        next_play_played_at,
        play_played_at_rank,
        ROUND(play_played_at_quartile / NULLIF(play_played_at_cume_dist, 0), 3) AS play_played_at_quartile_per_play_played_at_cume_
    FROM ranked
)
-- Final projection
SELECT
    id,
    device,
    is_skipped,
    play_id,
    play_device,
    play_played_at_cume_dist,
    play_played_at_tercile,
    next_play_played_at,
    play_played_at_rank,
    play_played_at_quartile_per_play_played_at_cume_
FROM annotated
WHERE play_played_at_rank < 7
ORDER BY play_played_at_rank DESC, id, play_id
LIMIT 20
