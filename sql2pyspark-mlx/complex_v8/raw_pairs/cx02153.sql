WITH cleaned_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        signup_date
    FROM listeners
    WHERE country NOT IN ('BR', 'UK')
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM cleaned_listeners s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
        )
      AND s.signup_date >= DATE '2024-07-13'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MAX(played_at) AS last_played_at,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.avg_play_ms_played
    FROM filtered s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        plan,
        signup_date,
        last_played_at,
        avg_play_ms_played,
        ROUND(AVG(avg_play_ms_played) OVER (PARTITION BY plan), 2) AS plan_avg_avg_play_ms_played,
        SUM(avg_play_ms_played) OVER (PARTITION BY plan) AS plan_sum_avg_play_ms_played
    FROM with_stats
    WINDOW w_plan AS (PARTITION BY plan)
)
-- Final projection
SELECT
    id,
    plan,
    signup_date,
    avg_play_ms_played,
    plan_avg_avg_play_ms_played,
    plan_sum_avg_play_ms_played,
    COALESCE(plan, 'UNKNOWN') AS plan_or_unknown,
    CONCAT(plan, '#', CAST(id AS STRING)) AS plan_ref
FROM with_windows
WHERE plan_sum_avg_play_ms_played BETWEEN 1203500.0 AND 3001250.0
ORDER BY plan, plan_or_unknown DESC NULLS FIRST
