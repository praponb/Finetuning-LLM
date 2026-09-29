WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE (is_skipped = FALSE OR track_id <= 20)
),
per_group AS (
    -- Group-level totals
    SELECT
        listener_id,
        COUNT(*) AS cnt,
        SUM(ms_played) AS total_ms_played,
        MIN(played_at) AS first_played_at,
        COUNT(DISTINCT device) AS distinct_devices
    FROM filtered_plays
    WHERE ms_played >= 95960
    GROUP BY listener_id
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM per_group s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.country IS NOT NULL
              AND l.plan NOT IN ('free', 'student')
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(total_ms_played) OVER (PARTITION BY listener_id, distinct_devices ORDER BY first_played_at, listener_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS total_ms_played_avg_3,
        ROUND(AVG(cnt) OVER (PARTITION BY distinct_devices, listener_id ORDER BY total_ms_played DESC NULLS FIRST, listener_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS cnt_avg_6_prior,
        NTH_VALUE(total_ms_played, 2) OVER (PARTITION BY listener_id, distinct_devices ORDER BY first_played_at, listener_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_ms_played,
        SUM(total_ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_total_ms_played
    FROM without_matches
)
SELECT
    listener_id,
    total_ms_played,
    distinct_devices,
    total_ms_played_avg_3,
    cnt_avg_6_prior,
    second_total_ms_played,
    ROUND(cnt_avg_6_prior * 0.25, 2) AS cnt_avg_6_prior_adj,
    FLOOR(second_total_ms_played / 1000000) * 1000000 AS second_total_ms_played_bucket
FROM metrics
WHERE listener_id_sum_total_ms_played < 16801500
