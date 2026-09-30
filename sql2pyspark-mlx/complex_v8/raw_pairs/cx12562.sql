WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
      AND term IS NOT NULL
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.level AS course_level,
        c.credits AS course_credits,
        c.course_code
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        course_level,
        course_credits,
        course_code,
        ROUND(AVG(course_credits) OVER (PARTITION BY student_id, course_level), 2) AS student_id_course_level_avg_course_credits,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_level, course_id), 0), 4) AS score_share_of_course_level_course_id,
        ROW_NUMBER() OVER (PARTITION BY grade_status, course_level ORDER BY course_credits NULLS LAST, id) AS rn,
        NTH_VALUE(course_credits, 2) OVER (PARTITION BY grade_status, course_level ORDER BY course_credits NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_credits,
        ROUND(PERCENT_RANK() OVER w_all_by_score_ties, 3) AS score_percent_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_credits)) AS distinct_grade_status_in_course_credits,
        ROUND(PERCENT_RANK() OVER w_all_by_score, 3) AS score_percent_rank_2,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, course_level), 2) AS score_vs_student_id_course_level_avg
    FROM joined_course
    WINDOW w_all_by_score_ties AS (ORDER BY score), w_all_by_score AS (ORDER BY score, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        course_level,
        course_credits,
        course_code,
        score_share_of_course_level_course_id,
        rn,
        second_course_credits,
        score_percent_rank,
        score_percent_rank_2,
        score_vs_student_id_course_level_avg,
        second_course_credits + score_percent_rank_2 AS second_course_credits_plus_score_percent_rank_2,
        score - score_percent_rank_2 AS score_minus_score_percent_rank_2
    FROM metrics
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    course_credits,
    course_code,
    score_share_of_course_level_course_id,
    rn,
    second_course_credits,
    score_percent_rank_2,
    score_vs_student_id_course_level_avg,
    second_course_credits_plus_score_percent_rank_2,
    score_percent_rank + second_course_credits AS score_percent_rank_plus_second_course_credits,
    COALESCE(score_percent_rank_2, 0.0) AS score_percent_rank_2_filled
FROM derived
ORDER BY course_credits, second_course_credits DESC
