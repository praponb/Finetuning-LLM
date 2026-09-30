WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-06'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.ms_played_filled,
        s.played_at_plus_15m,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM recent_plays s
    JOIN plays p ON p.track_id = s.track_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY play_id ORDER BY ms_played_filled, id, play_id) AS ms_played_filled_seq,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY play_listener_id), 2) AS play_listener_id_avg_ms_played_filled,
        SUM(ms_played_filled) OVER (PARTITION BY play_listener_id) AS play_listener_id_sum_ms_played_filled,
        RANK() OVER w_play_id_by_ms_played_filled_2 AS ms_played_filled_rank,
        COUNT(*) OVER w_play_listener_id AS rows_in_play_listener_id
    FROM with_children_plays
    WINDOW w_play_id_by_ms_played_filled_2 AS (PARTITION BY play_id ORDER BY ms_played_filled), w_play_listener_id AS (PARTITION BY play_listener_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, ms_played_filled, rows_in_play_listener_id, ms_played_filled_seq, ms_played
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric_name IN (ms_played_filled, rows_in_play_listener_id, ms_played_filled_seq, ms_played)
) unp
