WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CASE WHEN ms_played >= 225733 THEN 'large' WHEN ms_played >= 151112 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
),
joined_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        t.title AS track_title,
        t.bpm AS track_bpm,
        t.moods AS track_moods
    FROM cleaned s
    LEFT JOIN tracks t ON t.id = s.track_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(track_bpm) OVER w_track_id_listener_id, 2) AS track_id_listener_id_avg_track_bpm,
        MIN(track_bpm) OVER w_is_skipped AS is_skipped_min_track_bpm,
        NTILE(10) OVER (PARTITION BY listener_id, is_skipped ORDER BY track_bpm, id) AS track_bpm_decile,
        COUNT(*) OVER w_track_id AS rows_in_track_id,
        DENSE_RANK() OVER w_listener_id_by_track_bpm AS track_bpm_dense_rank,
        LEAD(track_bpm) OVER w_track_id_by_track_bpm AS next_track_bpm,
        LAST_VALUE(track_bpm) OVER (PARTITION BY listener_id, is_skipped ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_bpm,
        MAX(track_bpm) OVER (PARTITION BY track_id ORDER BY track_bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_track_bpm_so_far,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER w_track_id_listener_id, 0), 4) AS track_bpm_share_of_track_id_listener_id
    FROM joined_track
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id), w_is_skipped AS (PARTITION BY is_skipped), w_track_id AS (PARTITION BY track_id), w_listener_id_by_track_bpm AS (PARTITION BY listener_id ORDER BY track_bpm DESC, id), w_track_id_by_track_bpm AS (PARTITION BY track_id ORDER BY track_bpm ASC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE track_bpm_dense_rank <= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        track_title,
        track_bpm,
        track_id_listener_id_avg_track_bpm,
        track_bpm_decile,
        rows_in_track_id,
        next_track_bpm,
        track_bpm_share_of_track_id_listener_id,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_bpm_dense_rank, track_bpm_decile)) AS distinct_listener_id_in_track_bpm_dense_rank_tra,
        FIRST_VALUE(last_track_bpm) OVER (PARTITION BY listener_id ORDER BY last_track_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_track_bpm,
        MAX(max_track_bpm_so_far) OVER w_track_bpm_dense_rank_track_b AS track_bpm_dense_rank_track_bpm_decile_max_max_tr,
        NTH_VALUE(rows_in_track_id, 2) OVER (ORDER BY next_track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_track_id,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY last_track_bpm DESC) AS last_track_bpm_dense_rank,
        NTILE(10) OVER w_all_by_next_track_bpm AS next_track_bpm_decile
    FROM top_rows
    WINDOW w_track_bpm_dense_rank_track_b AS (PARTITION BY track_bpm_dense_rank, track_bpm_decile), w_all_by_next_track_bpm AS (ORDER BY next_track_bpm, id)
)
SELECT
    id,
    track_title,
    track_bpm,
    track_id_listener_id_avg_track_bpm,
    rows_in_track_id,
    track_bpm_share_of_track_id_listener_id,
    distinct_listener_id_in_track_bpm_dense_rank_tra,
    first_last_track_bpm,
    last_track_bpm_dense_rank,
    next_track_bpm_decile
FROM with_trends
WHERE rows_in_track_id <= 14
ORDER BY next_track_bpm_decile
