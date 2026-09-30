WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('|', device, source) AS device_source_label
    FROM plays
    WHERE source NOT IN ('playlist', 'album')
),
per_group AS (
    -- Summarise per group
    SELECT
        is_skipped,
        listener_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(ms_played), 2) AS avg_ms_played
    FROM scoped_plays
    GROUP BY 1, 2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, cnt ORDER BY cnt, is_skipped, listener_id), 3) AS cnt_cume_dist,
        FIRST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY is_skipped, cnt ORDER BY cnt, is_skipped, listener_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cnt,
        MIN(avg_ms_played) OVER w_listener_id AS listener_id_min_avg_ms_played,
        MIN(distinct_ids) OVER w_listener_id AS listener_id_min_distinct_ids,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY listener_id), 0), 4) AS cnt_share_of_listener_id
    FROM per_group
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.is_skipped,
        s.listener_id,
        s.cnt,
        s.avg_ms_played,
        s.listener_id_min_avg_ms_played,
        s.listener_id_min_distinct_ids,
        p.id AS play_id,
        p.device AS play_device,
        p.track_id AS play_track_id
    FROM metrics s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    is_skipped,
    listener_id,
    cnt,
    avg_ms_played,
    listener_id_min_avg_ms_played,
    listener_id_min_distinct_ids,
    play_id,
    play_device,
    play_track_id,
    CASE WHEN avg_ms_played >= 281500.0 THEN 'large' WHEN avg_ms_played >= 143250.0 THEN 'medium' ELSE 'small' END AS avg_ms_played_tier
FROM detail_rows_plays
ORDER BY play_track_id
