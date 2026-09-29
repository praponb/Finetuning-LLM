WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-25 10:00:00'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id)) AS distinct_source_in_track_id,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY YEAR(played_at) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        LAST_VALUE(source_len) IGNORE NULLS OVER (PARTITION BY YEAR(played_at) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_source_len,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(played_at) ORDER BY ms_played), 3) AS ms_played_cume_dist,
        ms_played - LAG(ms_played) OVER (ORDER BY played_at ASC, id) AS ms_played_change,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_source_device, 0), 4) AS ms_played_share_of_source_device,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY source, YEAR(played_at) ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        COUNT(*) OVER w_source_device AS rows_in_source_device,
        LEAD(played_at) OVER (PARTITION BY track_id, is_skipped ORDER BY source_len, id) AS next_played_at,
        RANK() OVER (PARTITION BY device, source_len ORDER BY source_len, id) AS source_len_rank
    FROM valid
    WINDOW w_source_device AS (PARTITION BY source, device)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(second_ms_played, 0) AS second_ms_played_filled,
        ABS(source_len_rank - 6) AS source_len_rank_distance
    FROM with_trends
),
summary AS (
    -- Group-level totals
    SELECT
        distinct_source_in_track_id,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        MAX(played_at) AS last_played_at,
        COUNT(*) FILTER (WHERE is_skipped = FALSE) AS n_is_skipped_false
    FROM labeled
    GROUP BY 1
    HAVING MAX(ms_played) > 99376
)
SELECT distinct_source_in_track_id, last_played_at, n_is_skipped_false
FROM summary
ORDER BY n_is_skipped_false
