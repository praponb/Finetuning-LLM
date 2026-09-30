WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE term NOT IN ('2024T2', '2025T1')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_ref,
        c.course_code,
        c.credits AS course_credits,
        c.level AS course_level
    FROM valid_enrollments s
    JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(course_credits, 2) OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_credits,
        LEAD(enrolled_at) OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at NULLS LAST, id) AS next_enrolled_at,
        COUNT(*) OVER (PARTITION BY course_id, student_id) AS rows_in_course_id_student_id,
        FIRST_VALUE(score) OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        LAST_VALUE(course_credits) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_course_credits,
        DENSE_RANK() OVER w_enrolled_at_month_by_enrolle AS enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY course_credits, id), 3) AS course_credits_cume_dist,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY course_id, student_id)) AS distinct_course_level_in_course_id_student_id,
        RANK() OVER w_enrolled_at_month_by_enrolle AS enrolled_at_rank,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id) AS score_dense_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_level, student_id)) AS distinct_grade_status_in_course_level_student_id,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_level, course_id ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM enriched
    WINDOW w_enrolled_at_month_by_enrolle AS (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE enrolled_at_dense_rank = 1
      AND second_enrolled_at >= TIMESTAMP '2025-01-25 14:00:00'
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    grade_status_ref,
    course_code,
    course_credits,
    second_course_credits,
    next_enrolled_at,
    rows_in_course_id_student_id,
    enrolled_at_dense_rank,
    course_credits_cume_dist,
    distinct_course_level_in_course_id_student_id,
    first_enrolled_at,
    score_dense_rank,
    distinct_grade_status_in_course_level_student_id,
    second_enrolled_at,
    GREATEST(enrolled_at_dense_rank, second_course_credits) AS greatest_enrolled_at_dense_rank_second_course_cr
FROM top_rows
ORDER BY course_code, second_course_credits DESC
