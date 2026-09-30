WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE ms_played > 145720
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        p.id AS play_id,
        p.source AS play_source,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM valid s
    JOIN plays p ON p.listener_id = s.listener_id AND p.track_id >= 23
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        play_id,
        play_track_id,
        play_device,
        ROW_NUMBER() OVER (PARTITION BY play_source, YEAR(play_played_at) ORDER BY play_played_at DESC, id, play_id) AS seq_no,
        NTILE(3) OVER w_play_source_play_played_at_y AS play_played_at_tercile,
        ROUND((UNIX_TIMESTAMP(play_played_at) - UNIX_TIMESTAMP(LAG(play_played_at) OVER w_listener_id_play_id_by_play)) / 60, 1) AS minutes_since_prev_play_played_at,
        FIRST_VALUE(play_played_at) OVER (PARTITION BY play_source, YEAR(play_played_at) ORDER BY play_played_at DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_play_played_at,
        ROW_NUMBER() OVER (PARTITION BY listener_id, play_id ORDER BY play_played_at ASC, id, play_id) AS seq_no_2,
        LEAD(play_played_at, 3) OVER (PARTITION BY listener_id, play_id ORDER BY play_played_at ASC, id, play_id) AS next3_play_played_at
    FROM detail_rows
    WINDOW w_play_source_play_played_at_y AS (PARTITION BY play_source, YEAR(play_played_at) ORDER BY play_played_at DESC, id, play_id), w_listener_id_play_id_by_play AS (PARTITION BY listener_id, play_id ORDER BY play_played_at ASC, id, play_id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE seq_no <= 2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_track_id, play_device, seq_no_2
    FROM top_per_group
) src
PIVOT (
    MIN(seq_no_2)
    FOR play_device IN ('desktop', 'speaker', 'tv')
)
