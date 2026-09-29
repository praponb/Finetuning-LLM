WITH base_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY played_at, id), 3) AS played_at_cume_dist,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, source)) AS distinct_track_id_in_listener_id_source,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        DENSE_RANK() OVER w_track_id_by_played_at AS played_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY played_at), 3) AS played_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER w_track_id_by_played_at, 3) AS played_at_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id_source, 0), 4) AS ms_played_share_of_listener_id_source,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, source), 2) AS listener_id_source_avg_ms_played,
        ROW_NUMBER() OVER (ORDER BY played_at NULLS LAST, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY listener_id, source) AS rows_in_listener_id_source,
        MAX(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2
    FROM base_plays
    WINDOW w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at, id), w_listener_id_source AS (PARTITION BY listener_id, source)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE seq_no = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        distinct_track_id_in_listener_id_source,
        first_known_ms_played,
        played_at_cume_dist_2,
        seq_no,
        MAX(played_at_percent_rank) OVER (PARTITION BY listener_id ORDER BY ms_played_share_of_listener_id_source DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS played_at_percent_rank_max_6_prior,
        LEAD(first_known_ms_played) OVER w_listener_id_by_ms_played_sha_2 AS next_first_known_ms_played,
        LEAD(rows_in_listener_id_source, 2) OVER (ORDER BY played_at, id) AS next2_rows_in_listener_id_source,
        SUM(ms_played) OVER w_source AS source_sum_ms_played,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source
    FROM top_rows
    WINDOW w_listener_id_by_ms_played_sha_2 AS (PARTITION BY listener_id ORDER BY ms_played_share_of_listener_id_source DESC, id), w_source AS (PARTITION BY source)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        SUM(CASE WHEN source <> 'playlist' THEN ms_played ELSE 0 END) AS ms_played_source_playlist,
        PERCENTILE_APPROX(source_sum_ms_played, 0.5) AS median_source_sum_ms_played,
        SUM(first_known_ms_played) AS total_first_known_ms_played,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS cnt
    FROM ranked
    GROUP BY 1
)
-- Final projection
SELECT
    played_at_ym,
    ms_played_source_playlist,
    last_played_at,
    cnt,
    DATE_FORMAT(last_played_at, 'yyyy-MM-dd') AS last_played_at_label,
    FLOOR(cnt / 2) * 2 AS cnt_bucket
FROM rollup_base
ORDER BY cnt DESC
