WITH base AS (
    -- Keep valid rows and derive helper columns
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
    WHERE track_id >= 12
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ms_played,
        source,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped), 2) AS is_skipped_avg_ms_played,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, source ORDER BY ms_played DESC), 3) AS ms_played_cume_dist,
        MAX(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_max_ms_played,
        NTILE(3) OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id) AS ms_played_tercile,
        SUM(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_sum_ms_played,
        LAG(played_at, 2) OVER (PARTITION BY track_id, source ORDER BY ms_played DESC, id) AS prev2_played_at,
        LEAD(ms_played, 3) OVER w_all_by_ms_played AS next3_ms_played,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        NTILE(10) OVER (PARTITION BY track_id, source ORDER BY ms_played DESC, id) AS ms_played_decile
    FROM base
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played, id)
)
-- Final projection
SELECT
    id,
    ms_played,
    source,
    is_skipped_avg_ms_played,
    rows_in_is_skipped,
    ms_played_cume_dist,
    is_skipped_sum_ms_played,
    prev2_played_at,
    next3_ms_played,
    ms_played_decile
FROM with_windows
WHERE next3_ms_played < 223262
ORDER BY rows_in_is_skipped DESC NULLS FIRST
