WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 12:00:00'
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
        is_skipped,
        source,
        device_slug,
        ROUND(CUME_DIST() OVER (PARTITION BY device ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        LAG(ms_played, 3) OVER w_track_id_played_at_month_by AS prev3_ms_played,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY played_at DESC) AS played_at_dense_rank,
        COUNT(*) OVER w_is_skipped_track_id AS rows_in_is_skipped_track_id,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_is_skipped_track_id, 0), 4) AS ms_played_share_of_is_skipped_track_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY TO_DATE(played_at) ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY is_skipped, track_id)) AS distinct_source_in_is_skipped_track_id,
        ms_played - LAG(ms_played) OVER (PARTITION BY TO_DATE(played_at) ORDER BY ms_played DESC, id) AS ms_played_change,
        DENSE_RANK() OVER w_track_id_played_at_month_by_2 AS played_at_dense_rank_2
    FROM prepared_plays
    WINDOW w_track_id_played_at_month_by AS (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at) ORDER BY played_at ASC NULLS LAST, id), w_is_skipped_track_id AS (PARTITION BY is_skipped, track_id), w_track_id_played_at_month_by_2 AS (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at) ORDER BY played_at ASC NULLS LAST)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        device_slug,
        played_at_cume_dist,
        prev3_ms_played,
        rows_in_is_skipped_track_id,
        ms_played_share_of_is_skipped_track_id,
        ms_played_avg_7,
        last_known_ms_played,
        distinct_source_in_is_skipped_track_id,
        ms_played_change,
        played_at_dense_rank_2
    FROM ranked
    WHERE played_at_dense_rank <= 2
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    device,
    is_skipped,
    source,
    device_slug,
    played_at_cume_dist,
    rows_in_is_skipped_track_id,
    last_known_ms_played,
    distinct_source_in_is_skipped_track_id,
    ms_played_change,
    played_at_dense_rank_2,
    CASE WHEN played_at_dense_rank_2 >= 8 THEN 'HIGH' WHEN played_at_dense_rank_2 >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS played_at_dense_rank_2_tier,
    played_at + INTERVAL 7 DAYS AS played_at_plus_7d
FROM top_per_group
ORDER BY rows_in_is_skipped_track_id DESC NULLS FIRST
