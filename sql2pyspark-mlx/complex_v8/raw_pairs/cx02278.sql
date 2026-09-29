WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE (source IS NOT NULL OR ms_played > 160264)
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND HOUR(p.played_at) BETWEEN 7 AND 13
              AND p.device IS NOT NULL
        )
),
deduped AS (
    -- Keep the latest row per source
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        is_skipped_flag,
        played_at_quarter,
        ms_played_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played DESC, id DESC) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        is_skipped_flag,
        played_at_quarter,
        ms_played_bucket,
        MAX(ms_played) OVER (PARTITION BY MONTH(played_at_quarter), track_id) AS played_at_quarter_month_track_id_max_ms_played,
        LAST_VALUE(played_at_quarter) IGNORE NULLS OVER (PARTITION BY is_skipped, device ORDER BY played_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_quarter,
        DENSE_RANK() OVER (PARTITION BY is_skipped, device ORDER BY played_at_quarter DESC) AS played_at_quarter_dense_rank
    FROM deduped
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.is_skipped_flag,
        s.played_at_quarter_dense_rank,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM with_windows s
    JOIN plays p ON p.track_id = s.track_id AND p.played_at >= TIMESTAMP '2025-01-19 13:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        ms_played,
        source,
        play_id,
        play_played_at,
        play_listener_id,
        ROUND(is_skipped_flag - AVG(is_skipped_flag) OVER (PARTITION BY track_id), 2) AS is_skipped_flag_vs_track_id_avg,
        SIZE(COLLECT_SET(play_listener_id) OVER w_track_id) AS distinct_play_listener_id_in_track_id,
        SUM(played_at_quarter_dense_rank) OVER (PARTITION BY track_id) AS track_id_sum_played_at_quarter_dense_rank,
        ROUND(AVG(played_at_quarter_dense_rank) OVER w_track_id, 2) AS track_id_avg_played_at_quarter_dense_rank,
        RANK() OVER (PARTITION BY played_at_quarter_dense_rank, play_id ORDER BY ms_played DESC, id, play_id) AS ms_played_rank
    FROM detail_rows
    WINDOW w_track_id AS (PARTITION BY track_id)
)
-- Final projection
SELECT
    id,
    source,
    play_id,
    play_played_at,
    play_listener_id,
    is_skipped_flag_vs_track_id_avg,
    track_id_sum_played_at_quarter_dense_rank,
    track_id_avg_played_at_quarter_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
    LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug_2
FROM with_trends
ORDER BY track_id_sum_played_at_quarter_dense_rank NULLS LAST, source DESC NULLS FIRST
