WITH prepared_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        CASE WHEN plan IN ('student', 'free') THEN 'group_a' WHEN plan = 'premium' THEN 'group_b' ELSE 'other' END AS plan_group
    FROM listeners
    WHERE (username IS NOT NULL OR YEAR(signup_date) = 2025)
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.listener_id = s.id) AS avg_play_ms_played,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.listener_id = s.id) AS avg_play_ms_played_2
    FROM prepared_listeners s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests,
        avg_play_ms_played,
        avg_play_ms_played_2,
        NTILE(4) OVER (PARTITION BY country, plan ORDER BY avg_play_ms_played_2, id) AS avg_play_ms_played_2_quartile,
        NTILE(5) OVER (PARTITION BY plan_group ORDER BY birth_year DESC, id) AS birth_year_quintile,
        NTH_VALUE(avg_play_ms_played, 2) OVER (PARTITION BY country, plan ORDER BY avg_play_ms_played_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_play_ms_played,
        SIZE(COLLECT_SET(plan) OVER w_country) AS distinct_plan_in_country
    FROM with_counts
    WINDOW w_country AS (PARTITION BY country)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY avg_play_ms_played_2_quartile, birth_year_quintile ORDER BY second_avg_play_ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        NTILE(4) OVER w_avg_play_ms_played_2_quartil_2 AS second_avg_play_ms_played_quartile,
        NTILE(10) OVER (ORDER BY signup_date, id) AS signup_date_decile
    FROM windowed
    WINDOW w_avg_play_ms_played_2_quartil_2 AS (PARTITION BY avg_play_ms_played_2_quartile, birth_year_quintile ORDER BY second_avg_play_ms_played NULLS LAST, id)
)
SELECT
    birth_year_quintile,
    plan,
    signup_date_decile,
    GROUPING(birth_year_quintile) AS is_total_birth_year_quintile,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY GROUPING SETS ((birth_year_quintile, plan, signup_date_decile), (birth_year_quintile), ())
ORDER BY birth_year_quintile NULLS LAST, plan NULLS LAST, signup_date_decile NULLS LAST
