WITH base AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'indie')
      AND plan = 'student'
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.listener_id = s.id) AS min_play_ms_played,
        (SELECT COUNT(CASE WHEN NOT p.is_skipped THEN 1 END) FROM plays p WHERE p.listener_id = s.id) AS play_is_skipped_count,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.listener_id = s.id) AS total_play_ms_played
    FROM base s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        username,
        plan,
        country,
        interests,
        play_is_skipped_count,
        total_play_ms_played,
        ROUND(AVG(play_is_skipped_count) OVER (PARTITION BY play_is_skipped_count ORDER BY birth_year DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS play_is_skipped_count_avg_3,
        ROUND(CUME_DIST() OVER (ORDER BY total_play_ms_played ASC, id), 3) AS total_play_ms_played_cume_dist,
        total_play_ms_played - LAG(total_play_ms_played) OVER w_play_is_skipped_count_by_bir_2 AS total_play_ms_played_change,
        FIRST_VALUE(birth_year) OVER (ORDER BY total_play_ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_birth_year
    FROM with_stats
    WINDOW w_play_is_skipped_count_by_bir_2 AS (PARTITION BY play_is_skipped_count ORDER BY birth_year DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        plan,
        interests,
        play_is_skipped_count,
        play_is_skipped_count_avg_3,
        COUNT(*) OVER (PARTITION BY country, play_is_skipped_count ORDER BY first_birth_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY country, play_is_skipped_count ORDER BY first_birth_year NULLS LAST, id) AS first_birth_year_seq,
        RANK() OVER w_country_plan_by_total_play_m AS total_play_ms_played_change_rank
    FROM sequenced
    WINDOW w_country_plan_by_total_play_m AS (PARTITION BY country, plan ORDER BY total_play_ms_played_change, id)
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MIN(played_at) AS first_played_at,
        COUNT(CASE WHEN device NOT IN ('phone', 'tv') THEN 1 END) AS play_device_count
    FROM plays
    GROUP BY listener_id
),
with_stats_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM metrics s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at
    FROM with_stats_2 s
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(last_played_at, 'yyyy-MM') AS last_played_at_ym,
        TO_DATE(first_played_at) AS first_played_at_day,
        ROUND(SUM(play_is_skipped_count) / NULLIF(SUM(first_birth_year_seq), 0), 3) AS play_is_skipped_count_to_first_birth_year_seq_ra,
        MAX(play_is_skipped_count_avg_3) AS max_play_is_skipped_count_avg_3,
        SUM(total_play_ms_played_change_rank) AS total_total_play_ms_played_change_rank
    FROM profiled
    GROUP BY 1, 2
)
-- Final projection
SELECT
    last_played_at_ym,
    first_played_at_day,
    play_is_skipped_count_to_first_birth_year_seq_ra,
    max_play_is_skipped_count_avg_3,
    total_total_play_ms_played_change_rank,
    LOWER(REGEXP_REPLACE(TRIM(last_played_at_ym), ' ', '_')) AS last_played_at_ym_slug,
    LOWER(REGEXP_REPLACE(TRIM(last_played_at_ym), ' ', '_')) AS last_played_at_ym_slug_2
FROM summary
WHERE total_total_play_ms_played_change_rank >= 13
