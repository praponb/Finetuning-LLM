WITH base_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LENGTH(TRIM(level)) AS level_len,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE credits < 3
      AND level NOT IN ('advanced', 'core')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        dept,
        level,
        level_len,
        credits_distance,
        LAST_VALUE(credits_distance) IGNORE NULLS OVER (PARTITION BY dept ORDER BY credits NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits_distance,
        MIN(credits) OVER w_credits_level_len AS credits_level_len_min_credits,
        MAX(level_len) OVER (PARTITION BY credits_distance ORDER BY credits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS level_len_max_4,
        ROUND(credits_distance / NULLIF(SUM(credits_distance) OVER (PARTITION BY credits, level_len), 0), 4) AS credits_distance_share_of_credits_level_len,
        FIRST_VALUE(level_len) OVER (PARTITION BY credits_distance ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level_len,
        NTH_VALUE(credits, 2) OVER (PARTITION BY credits_distance, dept ORDER BY credits_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        RANK() OVER (PARTITION BY dept ORDER BY credits NULLS LAST) AS credits_rank
    FROM base_courses
    WINDOW w_credits_level_len AS (PARTITION BY credits, level_len)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(first_level_len) OVER (PARTITION BY second_credits), 2) AS second_credits_avg_first_level_len,
        LAG(level_len) OVER (PARTITION BY first_level_len, credits_distance ORDER BY last_known_credits_distance, id) AS prev_level_len,
        ROW_NUMBER() OVER (PARTITION BY first_level_len, credits_distance ORDER BY last_known_credits_distance, id) AS seq_no,
        NTH_VALUE(level_len_max_4, 2) OVER (PARTITION BY first_level_len, credits_distance ORDER BY last_known_credits_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level_len_max_4
    FROM with_trends
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        credits_distance,
        second_credits,
        COUNT(*) FILTER (WHERE level IN ('core', 'intro')) AS n_level_in_core_intro,
        PERCENTILE_APPROX(second_level_len_max_4, 0.5) AS median_second_level_len_max_4,
        COUNT(*) AS row_count,
        SUM(level_len) AS total_level_len,
        MIN(last_known_credits_distance) AS min_last_known_credits_distance
    FROM sequenced
    WHERE last_known_credits_distance > 1
    GROUP BY credits_distance, second_credits
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(credits_distance / NULLIF(SUM(credits_distance) OVER (PARTITION BY row_count, min_last_known_credits_distance), 0), 4) AS credits_distance_share_of_row_count_min_last_kno,
        RANK() OVER w_all_by_n_level_in_core_intro AS n_level_in_core_intro_rank
    FROM per_group
    WINDOW w_all_by_n_level_in_core_intro AS (ORDER BY n_level_in_core_intro DESC)
)
SELECT
    credits_distance,
    n_level_in_core_intro,
    total_level_len,
    min_last_known_credits_distance,
    credits_distance_share_of_row_count_min_last_kno,
    n_level_in_core_intro_rank,
    GREATEST(row_count, n_level_in_core_intro) AS greatest_row_count_n_level_in_core_intro
FROM metrics
ORDER BY total_level_len DESC, greatest_row_count_n_level_in_core_intro DESC NULLS FIRST
