WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 15:00:00'
),
summary AS (
    -- Group-level totals
    SELECT
        track_id,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        SUM(ms_played) AS total_ms_played
    FROM recent
    WHERE is_skipped = TRUE
    GROUP BY track_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        track_id,
        stddev_ms_played,
        total_ms_played,
        total_ms_played - LAG(total_ms_played) OVER (ORDER BY total_ms_played DESC, track_id) AS total_ms_played_change,
        LAST_VALUE(total_ms_played) OVER (ORDER BY total_ms_played DESC, track_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_ms_played,
        ROUND(total_ms_played - AVG(total_ms_played) OVER (PARTITION BY track_id), 2) AS total_ms_played_vs_track_id_avg,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id
    FROM summary
),
with_parent_track AS (
    -- Attach track attributes
    SELECT
        s.track_id,
        s.stddev_ms_played,
        s.total_ms_played,
        s.total_ms_played_change,
        s.last_total_ms_played,
        s.total_ms_played_vs_track_id_avg,
        s.rows_in_track_id,
        t.artist_id AS track_artist_id
    FROM with_windows s
    LEFT JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        track_id,
        stddev_ms_played,
        total_ms_played,
        total_ms_played_change,
        last_total_ms_played,
        total_ms_played_vs_track_id_avg,
        rows_in_track_id,
        track_artist_id,
        COUNT(*) OVER (PARTITION BY track_artist_id) AS rows_in_track_artist_id,
        NTH_VALUE(total_ms_played, 2) OVER (ORDER BY total_ms_played_vs_track_id_avg DESC NULLS FIRST, track_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_ms_played,
        MAX(last_total_ms_played) OVER (PARTITION BY track_artist_id) AS track_artist_id_max_last_total_ms_played,
        SUM(rows_in_track_id) OVER (PARTITION BY track_artist_id) AS track_artist_id_sum_rows_in_track_id,
        ROW_NUMBER() OVER w_all_by_total_ms_played_vs_tr_2 AS seq_no
    FROM with_parent_track
    WINDOW w_all_by_total_ms_played_vs_tr_2 AS (ORDER BY total_ms_played_vs_track_id_avg DESC NULLS FIRST, track_id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        track_id,
        total_ms_played_change,
        last_total_ms_played,
        rows_in_track_id,
        rows_in_track_artist_id,
        second_total_ms_played,
        track_artist_id_max_last_total_ms_played,
        track_artist_id_sum_rows_in_track_id,
        seq_no,
        GREATEST(second_total_ms_played, rows_in_track_id) AS greatest_second_total_ms_played_rows_in_track_id,
        ABS(rows_in_track_id - 7) AS rows_in_track_id_distance
    FROM ranked
    WHERE track_artist_id >= 9
)
-- Final projection
SELECT
    track_id,
    total_ms_played_change,
    last_total_ms_played,
    rows_in_track_id,
    second_total_ms_played,
    track_artist_id_sum_rows_in_track_id,
    seq_no,
    greatest_second_total_ms_played_rows_in_track_id,
    rows_in_track_id_distance
FROM labeled
ORDER BY rows_in_track_id_distance NULLS LAST, track_id
