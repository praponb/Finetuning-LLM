WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE term = '2024T1'
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        c.level AS course_level
    FROM recent_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        score,
        course_level,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_level ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        SUM(score) OVER (PARTITION BY course_level ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_sum_1_prior,
        LEAD(score) OVER (PARTITION BY student_id ORDER BY score DESC, id) AS next_score,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_level, term)) AS distinct_student_id_in_course_level_term,
        score - LAG(score) OVER (ORDER BY score DESC, id) AS score_change,
        MAX(score) OVER (PARTITION BY term, course_id ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_max_4,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term, course_level)) AS distinct_course_id_in_term_course_level,
        SUM(score) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        MAX(score) OVER (PARTITION BY course_level, term) AS course_level_term_max_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_level ORDER BY score), 3) AS score_percent_rank,
        LEAD(score) OVER (PARTITION BY term, course_id ORDER BY score DESC NULLS FIRST, id) AS next_score_2,
        SUM(score) OVER (PARTITION BY term, course_id ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2
    FROM enriched_course
    WINDOW w_all_by_score AS (ORDER BY score DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(next_score_2 / 25) * 25 AS next_score_2_bucket,
        ABS(next_score - 57.3) AS next_score_distance
    FROM metrics
    WHERE score_percent_rank >= 0.2
)
SELECT
    id,
    course_id,
    term,
    score,
    second_score,
    score_sum_1_prior,
    next_score,
    distinct_student_id_in_course_level_term,
    score_change,
    score_max_4,
    distinct_course_id_in_term_course_level,
    running_score,
    course_level_term_max_score,
    score_percent_rank,
    next_score_2,
    next_score_2_bucket,
    next_score_distance,
    LEAST(score_change, score_max_4) AS least_score_change_score_max_4
FROM scored
ORDER BY term NULLS LAST, id
LIMIT 10
