WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND grade_status <> 'PASS'
),
with_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        c.dept AS course_dept,
        c.credits AS course_credits,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM scoped_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_course_credits AS seq_no,
        SUM(course_credits) OVER (PARTITION BY student_cohort, course_id) AS student_cohort_course_id_sum_course_credits,
        DENSE_RANK() OVER (PARTITION BY student_id, term ORDER BY course_credits DESC NULLS FIRST) AS course_credits_dense_rank,
        ROUND(AVG(course_credits) OVER (PARTITION BY student_cohort, course_id), 2) AS student_cohort_course_id_avg_course_credits,
        FIRST_VALUE(course_credits) OVER (ORDER BY course_credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        FIRST_VALUE(course_credits) OVER (PARTITION BY student_id, term ORDER BY course_credits DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits_2,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY course_credits), 2) AS course_credits_vs_course_credits_avg,
        ROUND(AVG(course_credits) OVER (PARTITION BY term, course_id ORDER BY course_credits, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS course_credits_avg_2,
        SIZE(COLLECT_SET(student_major) OVER (PARTITION BY course_credits)) AS distinct_student_major_in_course_credits,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_cohort, course_id)) AS distinct_term_in_student_cohort_course_id,
        LAG(course_credits, 2, 0) OVER (PARTITION BY term, course_id ORDER BY course_credits, id) AS prev2_course_credits,
        SUM(course_credits) OVER (PARTITION BY student_id, term ORDER BY course_credits DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_course_credits,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, course_id ORDER BY course_credits), 3) AS course_credits_percent_rank,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY course_credits), 0), 4) AS course_credits_share_of_course_credits,
        MIN(course_credits) OVER (ORDER BY course_credits ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_course_credits_so_far
    FROM with_details
    WINDOW w_all_by_course_credits AS (ORDER BY course_credits ASC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE seq_no = 1
)
SELECT
    id,
    course_id,
    course_dept,
    student_major,
    student_cohort,
    student_cohort_course_id_avg_course_credits,
    first_course_credits,
    first_course_credits_2,
    course_credits_vs_course_credits_avg,
    course_credits_avg_2,
    distinct_student_major_in_course_credits,
    prev2_course_credits,
    running_course_credits,
    course_credits_percent_rank,
    course_credits_share_of_course_credits,
    min_course_credits_so_far,
    COALESCE(course_dept, 'UNKNOWN') AS course_dept_or_unknown,
    SUBSTRING(TRIM(student_major), 1, 3) AS student_major_prefix
FROM leaders
WHERE student_major = 'Biz'
ORDER BY distinct_student_major_in_course_credits DESC NULLS FIRST
