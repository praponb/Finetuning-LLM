WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.credits AS course_credits
    FROM cleaned s
    JOIN courses c ON c.id = s.course_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        ROUND(AVG(course_credits) OVER (ORDER BY course_credits, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_2_prior,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY course_credits)) AS distinct_course_id_in_course_credits,
        MAX(course_credits) OVER (PARTITION BY student_id, term ORDER BY course_credits DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS course_credits_max_4,
        COUNT(*) OVER (PARTITION BY course_credits) AS rows_in_course_credits,
        COUNT(*) OVER (ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(course_credits) OVER (PARTITION BY course_credits) AS course_credits_sum_course_credits,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, grade_status ORDER BY course_credits NULLS LAST), 3) AS course_credits_percent_rank,
        ROUND(AVG(course_credits) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_3_prior,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_credits), 2) AS course_credits_avg_course_credits,
        SUM(course_credits) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS course_credits_sum_4,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_credits)) AS distinct_student_id_in_course_credits,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_credits)) AS distinct_grade_status_in_course_credits
    FROM combined_details_course
)
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    course_credits_avg_2_prior,
    course_credits_max_4,
    rows_in_course_credits,
    running_count,
    rn,
    course_credits_avg_3_prior,
    course_credits_avg_course_credits,
    course_credits_sum_4,
    distinct_student_id_in_course_credits,
    LENGTH(TRIM(term)) AS term_len
FROM windowed
WHERE term NOT IN ('2025T2', '2025T1')
