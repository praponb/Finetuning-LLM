WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T1'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept,
        c.course_code,
        c.credits AS course_credits
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept = 'Math'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term, grade_status ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        RANK() OVER w_course_dept_by_course_credit AS course_credits_rank,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY term), 2) AS course_credits_vs_term_avg,
        MIN(course_credits) OVER (PARTITION BY course_credits, DATE_TRUNC('MONTH', enrolled_at)) AS course_credits_enrolled_at_month_min_course_cred,
        NTILE(10) OVER (PARTITION BY course_dept ORDER BY course_credits, id) AS course_credits_decile,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term, course_id)) AS distinct_grade_status_in_term_course_id,
        SUM(course_credits) OVER (PARTITION BY course_dept ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_course_credits
    FROM combined_details
    WINDOW w_course_dept_by_course_credit AS (PARTITION BY course_dept ORDER BY course_credits)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        course_credits,
        course_credits_rank,
        course_credits_vs_term_avg,
        course_credits_decile,
        first_enrolled_at,
        running_course_credits,
        DENSE_RANK() OVER (PARTITION BY grade_status ORDER BY course_credits DESC) AS course_credits_dense_rank,
        SUM(running_course_credits) OVER (PARTITION BY grade_status) AS grade_status_sum_running_course_credits,
        NTH_VALUE(course_credits_decile, 2) OVER (PARTITION BY distinct_grade_status_in_term_course_id ORDER BY distinct_grade_status_in_term_course_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_credits_decile,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY first_known_enrolled_at, id) AS rn,
        LAG(course_credits_vs_term_avg, 2) OVER w_term_by_first_known_enrolled AS prev2_course_credits_vs_term_avg,
        SUM(course_credits_decile) OVER (PARTITION BY grade_status) AS grade_status_sum_course_credits_decile,
        ROUND(AVG(course_credits_decile) OVER (PARTITION BY grade_status ORDER BY course_credits DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS course_credits_decile_avg_6_prior
    FROM with_windows
    WINDOW w_term_by_first_known_enrolled AS (PARTITION BY term ORDER BY first_known_enrolled_at, id)
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    enrolled_at,
    course_credits,
    course_credits_rank,
    course_credits_decile,
    first_enrolled_at,
    running_course_credits,
    course_credits_dense_rank,
    grade_status_sum_running_course_credits,
    second_course_credits_decile,
    prev2_course_credits_vs_term_avg,
    course_credits_decile_avg_6_prior,
    ABS(course_credits_decile - 6) AS course_credits_decile_distance
FROM with_windows_2
ORDER BY grade_status_sum_running_course_credits, id
LIMIT 5
