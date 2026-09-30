WITH recent_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        country,
        signup_date,
        interests,
        SUBSTRING(TRIM(country), 1, 2) AS country_prefix,
        ABS(birth_year - 1980) AS birth_year_distance
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'live')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        interests,
        ROUND(birth_year_distance - AVG(birth_year_distance) OVER (PARTITION BY country), 2) AS birth_year_distance_vs_country_avg,
        MIN(birth_year_distance) OVER (PARTITION BY country) AS country_min_birth_year_distance,
        RANK() OVER (PARTITION BY country, YEAR(signup_date) ORDER BY signup_date DESC, id) AS signup_date_rank,
        ROUND(birth_year_distance / NULLIF(SUM(birth_year_distance) OVER w_country, 0), 4) AS birth_year_distance_share_of_country
    FROM recent_listeners
    WINDOW w_country AS (PARTITION BY country)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        interests,
        birth_year_distance_vs_country_avg,
        country_min_birth_year_distance,
        SUM(signup_date_rank) OVER (PARTITION BY signup_date_rank) AS signup_date_rank_sum_signup_date_rank,
        NTH_VALUE(signup_date_rank, 2) OVER (PARTITION BY country, signup_date_rank ORDER BY birth_year_distance_vs_country_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date_rank,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY signup_date_rank)) AS distinct_country_in_signup_date_rank,
        ROUND(signup_date_rank - AVG(signup_date_rank) OVER w_signup_date_rank, 2) AS signup_date_rank_vs_signup_date_rank_avg,
        SUM(signup_date_rank) OVER (PARTITION BY country, signup_date_rank ORDER BY birth_year_distance_vs_country_avg, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS signup_date_rank_sum_2_prior,
        LAST_VALUE(signup_date_rank) IGNORE NULLS OVER (PARTITION BY signup_date_rank ORDER BY birth_year_distance_vs_country_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_signup_date_rank
    FROM windowed
    WINDOW w_signup_date_rank AS (PARTITION BY signup_date_rank)
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.interests,
        s.birth_year_distance_vs_country_avg,
        s.signup_date_rank_sum_signup_date_rank,
        s.signup_date_rank_vs_signup_date_rank_avg,
        s.last_known_signup_date_rank,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM with_trends s
    LEFT JOIN plays p ON p.listener_id = s.id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(play_device) OVER w_play_id) AS distinct_play_device_in_play_id,
        MAX(last_known_signup_date_rank) OVER (PARTITION BY play_id, play_device ORDER BY signup_date_rank_sum_signup_date_rank NULLS LAST, id, play_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last_known_signup_date_rank_max_2_prior,
        LAG(last_known_signup_date_rank) OVER (PARTITION BY play_id, play_device ORDER BY signup_date_rank_sum_signup_date_rank NULLS LAST, id, play_id) AS prev_last_known_signup_date_rank,
        FIRST_VALUE(last_known_signup_date_rank) OVER (PARTITION BY play_id, play_device ORDER BY signup_date_rank_sum_signup_date_rank NULLS LAST, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_known_signup_date_rank,
        SIZE(COLLECT_SET(play_source) OVER (PARTITION BY play_id)) AS distinct_play_source_in_play_id
    FROM detail_rows
    WINDOW w_play_id AS (PARTITION BY play_id)
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.device = 'car'
              AND p.is_skipped = TRUE
        )
)
-- Final projection
SELECT
    id,
    interests,
    birth_year_distance_vs_country_avg,
    signup_date_rank_sum_signup_date_rank,
    signup_date_rank_vs_signup_date_rank_avg,
    last_known_signup_date_rank,
    play_is_skipped,
    play_device,
    last_known_signup_date_rank_max_2_prior,
    prev_last_known_signup_date_rank,
    distinct_play_source_in_play_id,
    INITCAP(TRIM(play_source)) AS play_source_clean
FROM without_matches
ORDER BY birth_year_distance_vs_country_avg DESC, prev_last_known_signup_date_rank DESC
