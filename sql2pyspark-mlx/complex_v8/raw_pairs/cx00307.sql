WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE listener_id IS NOT NULL
      AND source <> 'playlist'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ROW_NUMBER() OVER (ORDER BY played_at DESC, id) AS played_at_seq,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id) AS played_at_tercile,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id) AS played_at_seq_2
    FROM valid
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        played_at_seq,
        second_played_at,
        played_at_seq_2,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY MONTH(played_at), played_at_seq_2 ORDER BY played_at_seq_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2,
        MAX(played_at_seq) OVER (PARTITION BY played_at_seq_2) AS played_at_seq_2_max_played_at_seq,
        FIRST_VALUE(second_played_at) OVER (PARTITION BY MONTH(played_at), played_at_seq_2 ORDER BY played_at_seq_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_played_at,
        SUM(played_at_tercile) OVER (PARTITION BY MONTH(played_at), played_at_seq_2 ORDER BY played_at_seq_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_played_at_tercile,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(second_played_at) ORDER BY played_at_seq DESC, id) AS seq_no
    FROM ranked
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.second_played_at,
        s.second_played_at_2,
        s.first_second_played_at,
        s.running_played_at_tercile,
        s.seq_no,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.device AS play_device
    FROM sequenced s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device = 'desktop'
)
SELECT
    listener_id,
    seq_no,
    GROUPING(listener_id) AS is_total_listener_id,
    ROUND(STDDEV(running_played_at_tercile), 2) AS stddev_running_played_at_tercile
FROM detail_rows
GROUP BY ROLLUP(listener_id, seq_no)
