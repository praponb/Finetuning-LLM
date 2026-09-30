WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ABS(score - 43.5) AS score_distance
    FROM enrollments
    WHERE score BETWEEN 26.2 AND 74.9
      AND term NOT IN ('2024T1', '2024T2')
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_distance,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.level AS course_level
    FROM recent_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.level = 'core'
    WHERE c.dept <> 'Bio'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term ORDER BY score_distance NULLS LAST), 3) AS score_distance_percent_rank,
        RANK() OVER (PARTITION BY course_level ORDER BY course_credits) AS course_credits_rank,
        FIRST_VALUE(course_credits) IGNORE NULLS OVER (PARTITION BY course_level ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_course_credits,
        MAX(score) OVER (PARTITION BY course_id) AS course_id_max_score,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY term), 2) AS course_credits_vs_term_avg,
        COUNT(*) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(course_credits) OVER (PARTITION BY term ORDER BY score_distance NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_course_credits,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_level ORDER BY course_credits, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC), 3) AS score_percent_rank,
        MIN(score_distance) OVER (PARTITION BY course_id) AS course_id_min_score_distance,
        LEAD(course_credits) OVER w_all_by_enrolled_at AS next_course_credits,
        FIRST_VALUE(course_credits) IGNORE NULLS OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_course_credits_2,
        NTILE(5) OVER (ORDER BY enrolled_at ASC, id) AS enrolled_at_quintile,
        ROUND(CUME_DIST() OVER w_term_by_score_distance, 3) AS score_distance_cume_dist
    FROM combined_details
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at ASC, id), w_term_by_score_distance AS (PARTITION BY term ORDER BY score_distance NULLS LAST, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(first_known_course_credits_2) OVER w_next_course_credits AS next_course_credits_min_first_known_course_credi,
        ROUND(CUME_DIST() OVER (ORDER BY score_distance_percent_rank, id), 3) AS score_distance_percent_rank_cume_dist,
        LAST_VALUE(course_id_min_score_distance) OVER (ORDER BY score_distance_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_id_min_score_distance,
        RANK() OVER (ORDER BY score_distance_cume_dist) AS score_distance_cume_dist_rank
    FROM sequenced
    WINDOW w_next_course_credits AS (PARTITION BY next_course_credits)
)
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    score_distance,
    course_credits,
    course_dept,
    course_credits_rank,
    course_credits_vs_term_avg,
    running_count,
    minutes_since_prev_enrolled_at,
    score_percent_rank,
    next_course_credits,
    first_known_course_credits_2,
    enrolled_at_quintile,
    score_distance_cume_dist,
    next_course_credits_min_first_known_course_credi,
    score_distance_percent_rank_cume_dist,
    last_course_id_min_score_distance,
    score_distance_cume_dist_rank,
    score_distance + running_count AS score_distance_plus_running_count
FROM with_windows
ORDER BY score_distance_cume_dist_rank DESC
