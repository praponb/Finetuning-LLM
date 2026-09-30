WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
),
device_stats AS (
    -- Avg ms_played per device
    SELECT device, AVG(ms_played) AS avg_ms_played
    FROM valid_plays
    GROUP BY device
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM valid_plays s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played > g.avg_ms_played
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.avg_ms_played,
        s.ms_played_minus_avg,
        p.id AS play_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM above_average s
    JOIN plays p ON p.track_id = s.track_id
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.play_id,
        s.play_device,
        s.play_is_skipped,
        s.play_played_at,
        p.id AS play_id_2,
        p.played_at AS play_played_at_2,
        p.is_skipped AS play_is_skipped_2,
        p.track_id AS play_track_id
    FROM detail_rows_plays s
    JOIN plays p ON p.listener_id = s.listener_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        play_id,
        play_device,
        play_played_at,
        play_id_2,
        play_played_at_2,
        play_is_skipped_2,
        FIRST_VALUE(play_played_at_2) IGNORE NULLS OVER (PARTITION BY play_id, play_id_2 ORDER BY play_played_at_2 DESC, id, play_id, play_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_play_played_at_2,
        NTH_VALUE(play_played_at_2, 2) OVER (PARTITION BY play_id, play_id_2 ORDER BY play_played_at_2 DESC, id, play_id, play_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_play_played_at_2,
        ROW_NUMBER() OVER (PARTITION BY play_id, play_id_2 ORDER BY play_played_at_2 DESC, id, play_id, play_id_2) AS seq_no,
        LAST_VALUE(play_played_at) IGNORE NULLS OVER (PARTITION BY play_id, play_id_2 ORDER BY play_played_at_2 DESC, id, play_id, play_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY play_id, play_id_2 ORDER BY play_played_at_2 DESC), 3) AS play_played_at_2_percent_rank
    FROM with_children
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_played_at_2, play_played_at, play_device, seq_no
    FROM with_trends
) src
PIVOT (
    AVG(seq_no)
    FOR play_device IN ('phone', 'car')
)
ORDER BY play_played_at_2, play_played_at
