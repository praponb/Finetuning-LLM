WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE ms_played BETWEEN 112786 AND 261579
      AND played_at < TIMESTAMP '2025-02-18 12:00:00'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        listener_id,
        is_skipped,
        ROUND(SUM(ms_played) / NULLIF(SUM(source_len), 0), 3) AS ms_played_to_source_len_ratio,
        PERCENTILE_APPROX(source_len, 0.5) AS median_source_len
    FROM prepared_plays
    GROUP BY DATE_TRUNC('MONTH', played_at), listener_id, is_skipped
),
qualified AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM rollup_base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND SIZE(l.interests) >= 1
              AND l.plan IN ('family', 'student', 'premium')
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(median_source_len) OVER (ORDER BY ms_played_to_source_len_ratio, played_at_month, listener_id, is_skipped ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS median_source_len_max_3,
        DENSE_RANK() OVER (ORDER BY ms_played_to_source_len_ratio) AS ms_played_to_source_len_ratio_dense_rank,
        LAST_VALUE(median_source_len) OVER (ORDER BY ms_played_to_source_len_ratio, played_at_month, listener_id, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_median_source_len,
        FIRST_VALUE(median_source_len) OVER (PARTITION BY is_skipped, median_source_len ORDER BY ms_played_to_source_len_ratio ASC NULLS LAST, played_at_month, listener_id, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_median_source_len,
        ROUND(AVG(median_source_len) OVER (PARTITION BY listener_id, median_source_len ORDER BY ms_played_to_source_len_ratio, played_at_month, listener_id, is_skipped ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS median_source_len_avg_4
    FROM qualified
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = FALSE
        )
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        MAX(ms_played) AS max_play_ms_played,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.max_play_ms_played,
        p.last_played_at
    FROM without_matches s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
SELECT
    played_at_month,
    listener_id,
    is_skipped,
    median_source_len_max_3,
    ms_played_to_source_len_ratio_dense_rank,
    last_median_source_len,
    first_median_source_len,
    play_count,
    max_play_ms_played,
    last_played_at,
    FLOOR(median_source_len_max_3 / 5) * 5 AS median_source_len_max_3_bucket
FROM profiled
WHERE is_skipped = FALSE
ORDER BY median_source_len_max_3 NULLS LAST, last_median_source_len
