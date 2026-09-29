WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.prior_ms_played_7d,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.track_id AS play_track_id
    FROM in_window s
    JOIN plays p ON p.listener_id = s.listener_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM detail_rows s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY play_track_id ORDER BY prior_ms_played_7d ASC, id, play_id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY play_count, play_source ORDER BY first_played_at DESC, id, play_id) AS seq_no_2,
        COUNT(*) OVER (PARTITION BY play_id, play_source) AS rows_in_play_id_play_source,
        MIN(play_count) OVER (PARTITION BY play_count, play_source ORDER BY first_played_at DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_play_count_so_far
    FROM summarized_rows
)
-- Subtotals with ROLLUP
SELECT
    listener_id,
    play_count,
    play_id,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(seq_no_2), 2) AS stddev_seq_no_2,
    SUM(CASE WHEN play_source IN ('search', 'radio', 'playlist') THEN 1 ELSE 0 END) AS n_play_source_in_search_radio,
    ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list
FROM sequenced
GROUP BY ROLLUP(listener_id, play_count, play_id)
ORDER BY listener_id NULLS LAST, play_count NULLS LAST, play_id NULLS LAST
