WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE ms_played >= 56858
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, t.release_date AS track_release_date
    FROM cleaned_plays s
    LEFT JOIN tracks t ON t.id = s.track_id AND t.is_explicit = TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY listener_id ORDER BY track_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (ORDER BY track_release_date DESC, id) AS track_release_date_dense_rank,
        LAG(track_release_date) OVER (PARTITION BY listener_id ORDER BY track_release_date, id) AS prev_track_release_date,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device,
        ROUND(PERCENT_RANK() OVER w_all_by_track_release_date, 3) AS track_release_date_percent_rank,
        RANK() OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played DESC) AS ms_played_rank,
        ROUND(CUME_DIST() OVER (ORDER BY track_release_date DESC, id), 3) AS track_release_date_cume_dist,
        MAX(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', track_release_date)) AS track_release_date_month_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', track_release_date)), 2) AS track_release_date_month_avg_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_device, 0), 4) AS ms_played_share_of_device,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank,
        NTILE(3) OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played DESC, id) AS ms_played_tercile
    FROM enriched
    WINDOW w_all_by_track_release_date AS (ORDER BY track_release_date DESC, id), w_device AS (PARTITION BY device)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE track_release_date_dense_rank <= 5
      AND prev_track_release_date < DATE '2025-04-02'
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    track_release_date,
    prev_track_release_date,
    distinct_listener_id_in_device,
    ms_played_rank,
    track_release_date_cume_dist,
    track_release_date_month_avg_ms_played,
    ms_played_percent_rank,
    ms_played_tercile,
    FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
    ROUND(running_count / NULLIF(ms_played, 0), 3) AS running_count_per_ms_played
FROM leaders
WHERE ms_played_share_of_device BETWEEN 0.2 AND 0.7
