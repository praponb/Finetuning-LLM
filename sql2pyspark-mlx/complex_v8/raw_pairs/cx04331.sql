WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM listeners
    WHERE country NOT IN ('BR', 'TH')
      AND plan IS NOT NULL
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played
    FROM filtered s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
summarized_rows_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.device IS NOT NULL THEN 1 END) FROM plays p WHERE p.listener_id = s.id) AS play_device_count,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.id) AS play_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS first_played_at
    FROM summarized_rows s
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        min_play_ms_played + birth_year AS min_play_ms_played_plus_birth_year,
        LENGTH(TRIM(plan)) AS plan_len
    FROM summarized_rows_2
    WHERE play_count < 6
)
-- Subtotals with GROUPING SETS
SELECT
    play_device_count,
    plan,
    COUNT(DISTINCT id) AS distinct_ids,
    ARRAY_SORT(COLLECT_SET(country)) AS country_list,
    COUNT(DISTINCT plan) AS distinct_plans
FROM annotated
GROUP BY GROUPING SETS ((play_device_count, plan), (play_device_count), (plan), ())
ORDER BY play_device_count NULLS LAST, plan NULLS LAST
