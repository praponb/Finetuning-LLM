WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        ABS(ms_played - 178909) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE (device = 'phone' OR ms_played >= 44439)
),
latest AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        device_source_label,
        ms_played_distance,
        ms_played_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played_distance DESC, id DESC) AS row_num
        FROM prepared_plays
    ) ranked
    WHERE row_num = 1
),
per_group AS (
    -- Summarise per group
    SELECT
        track_id,
        ROUND(SUM(ms_played_distance) / NULLIF(SUM(ms_played_bucket), 0), 3) AS ms_played_distance_to_ms_played_bucket_ratio,
        COUNT(*) AS cnt,
        ROUND(STDDEV(ms_played_bucket), 2) AS stddev_ms_played_bucket,
        ROUND(SUM(ms_played_distance) / NULLIF(SUM(ms_played), 0), 3) AS ms_played_distance_to_ms_played_ratio
    FROM latest
    GROUP BY track_id
    HAVING MAX(ms_played) > 95960
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        track_id,
        ms_played_distance_to_ms_played_bucket_ratio,
        cnt,
        stddev_ms_played_bucket,
        ms_played_distance_to_ms_played_ratio,
        ROUND(cnt - AVG(cnt) OVER w_track_id_cnt, 2) AS cnt_vs_track_id_cnt_avg,
        RANK() OVER (PARTITION BY cnt, track_id ORDER BY ms_played_distance_to_ms_played_bucket_ratio DESC) AS ms_played_distance_to_ms_played_bucket_ratio_ran,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY track_id, cnt), 0), 4) AS cnt_share_of_track_id_cnt
    FROM per_group
    WINDOW w_track_id_cnt AS (PARTITION BY track_id, cnt)
),
annotated AS (
    -- Derive helper columns
    SELECT
        track_id,
        cnt,
        stddev_ms_played_bucket,
        ms_played_distance_to_ms_played_ratio,
        cnt_vs_track_id_cnt_avg,
        ms_played_distance_to_ms_played_bucket_ratio_ran,
        CASE WHEN cnt >= 9 THEN 'A' WHEN cnt >= 5 THEN 'B' ELSE 'C' END AS cnt_tier,
        COALESCE(cnt_vs_track_id_cnt_avg, 0.0) AS cnt_vs_track_id_cnt_avg_filled,
        cnt_vs_track_id_cnt_avg + cnt AS cnt_vs_track_id_cnt_avg_plus_cnt
    FROM ranked
    WHERE ms_played_distance_to_ms_played_bucket_ratio_ran <= 9
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT track_id, field, metric_value
FROM (
    SELECT track_id, ms_played_distance_to_ms_played_bucket_ratio_ran, cnt
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (ms_played_distance_to_ms_played_bucket_ratio_ran, cnt)
) unp
