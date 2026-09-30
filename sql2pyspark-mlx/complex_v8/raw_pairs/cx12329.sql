WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source IN ('playlist', 'search')
      AND NOT is_skipped
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(ms_played) OVER w_track_id AS track_id_max_ms_played,
        ROW_NUMBER() OVER (ORDER BY ms_played ASC, id) AS seq_no,
        LAG(played_at) OVER w_all_by_ms_played AS prev_played_at,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_device_track_id, 0), 4) AS ms_played_share_of_device_track_id,
        SUM(ms_played) OVER (PARTITION BY device, track_id) AS device_track_id_sum_ms_played,
        ROUND(CUME_DIST() OVER w_device_listener_id_by_ms_pla, 3) AS ms_played_cume_dist,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id, 2) AS ms_played_vs_track_id_avg,
        LAG(ms_played) OVER (PARTITION BY device, is_skipped ORDER BY ms_played DESC, id) AS prev_ms_played,
        RANK() OVER (PARTITION BY device, is_skipped ORDER BY ms_played DESC) AS ms_played_rank
    FROM base_plays
    WINDOW w_track_id AS (PARTITION BY track_id), w_all_by_ms_played AS (ORDER BY ms_played ASC, id), w_device_track_id AS (PARTITION BY device, track_id), w_device_listener_id_by_ms_pla AS (PARTITION BY device, listener_id ORDER BY ms_played ASC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM with_trends
    WHERE ms_played_rank <= 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY ms_played_cume_dist ASC, id), 3) AS ms_played_cume_dist_cume_dist,
        MAX(ms_played_vs_track_id_avg) OVER (PARTITION BY listener_id, track_id) AS listener_id_track_id_max_ms_played_vs_track_id_a,
        COUNT(*) OVER (PARTITION BY listener_id, track_id) AS rows_in_listener_id_track_id,
        ROUND(AVG(ms_played_cume_dist) OVER w_listener_id_track_id, 2) AS listener_id_track_id_avg_ms_played_cume_dist
    FROM top_per_group
    WINDOW w_listener_id_track_id AS (PARTITION BY listener_id, track_id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        device,
        MIN(played_at) AS first_played_at,
        MIN(prev_played_at) AS first_prev_played_at,
        ROUND(100 * COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_skipped_true,
        SUM(CASE WHEN device <> 'desktop' THEN listener_id_track_id_max_ms_played_vs_track_id_a ELSE 0 END) AS listener_id_track_id_max_ms_played_vs_track_id_a_2
    FROM with_windows
    WHERE ms_played_cume_dist <= 0.8
    GROUP BY 1, 2, 3
    HAVING MAX(seq_no) > 4
),
ranked AS (
    -- Previous / next values per key
    SELECT
        track_id,
        played_at_month,
        device,
        first_played_at,
        pct_is_skipped_true,
        listener_id_track_id_max_ms_played_vs_track_id_a_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(first_played_at), TO_DATE(played_at_month) ORDER BY first_played_at NULLS LAST), 3) AS first_played_at_percent_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(first_played_at), TO_DATE(played_at_month) ORDER BY first_played_at NULLS LAST, track_id, played_at_month, device ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(pct_is_skipped_true) OVER (PARTITION BY TO_DATE(first_played_at), TO_DATE(played_at_month) ORDER BY first_played_at NULLS LAST, track_id, played_at_month, device ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pct_is_skipped_true_max_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(first_played_at), TO_DATE(played_at_month) ORDER BY first_played_at NULLS LAST, track_id, played_at_month, device), 3) AS first_played_at_cume_dist
    FROM totals
)
-- Final projection
SELECT
    track_id,
    played_at_month,
    first_played_at,
    pct_is_skipped_true,
    listener_id_track_id_max_ms_played_vs_track_id_a_2,
    first_played_at_percent_rank,
    running_count,
    pct_is_skipped_true_max_2_prior,
    LENGTH(TRIM(device)) AS device_len,
    DATE_FORMAT(first_played_at, 'yyyy-MM-dd') AS first_played_at_label
FROM ranked
ORDER BY device_len
