WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_clean,
        c.level AS course_level,
        c.credits AS course_credits,
        c.dept AS course_dept,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(course_credits) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_course_credits,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY course_dept), 2) AS course_credits_vs_course_dept_avg,
        SUM(course_credits) OVER (PARTITION BY course_dept ORDER BY course_credits DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS course_credits_sum_3_prior,
        COUNT(*) OVER (PARTITION BY course_dept) AS rows_in_course_dept,
        DATEDIFF(student_enrolled_on, LAG(student_enrolled_on) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id)) AS days_since_prev_student_enrolled_on,
        course_credits - LAG(course_credits) OVER (PARTITION BY course_dept ORDER BY course_credits DESC, id) AS course_credits_change,
        ROUND(PERCENT_RANK() OVER w_course_id_student_cohort_by, 3) AS student_enrolled_on_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY course_dept ORDER BY course_credits DESC, id) AS rn,
        ROUND(AVG(course_credits) OVER (PARTITION BY DATE_TRUNC('MONTH', student_enrolled_on), course_id), 2) AS student_enrolled_on_month_course_id_avg_course_c,
        MAX(course_credits) OVER (PARTITION BY course_dept ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_course_credits_so_far,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY course_dept), 0), 4) AS course_credits_share_of_course_dept,
        MIN(course_credits) OVER w_course_dept AS course_dept_min_course_credits,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_dept)) AS distinct_grade_status_in_course_dept,
        LEAD(course_credits, 3) OVER w_course_id_student_cohort_by AS next3_course_credits
    FROM with_parent_course
    WINDOW w_course_id_student_cohort_by AS (PARTITION BY course_id, student_cohort ORDER BY student_enrolled_on NULLS LAST, id), w_course_dept AS (PARTITION BY course_dept)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        course_level,
        course_credits,
        student_enrolled_on,
        student_cohort,
        running_course_credits,
        course_credits_vs_course_dept_avg,
        course_credits_sum_3_prior,
        days_since_prev_student_enrolled_on,
        course_credits_change,
        student_enrolled_on_percent_rank,
        rn,
        student_enrolled_on_month_course_id_avg_course_c,
        max_course_credits_so_far,
        course_credits_share_of_course_dept,
        distinct_grade_status_in_course_dept,
        next3_course_credits,
        ABS(running_course_credits - 19) AS running_course_credits_distance,
        HOUR(enrolled_at) AS enrolled_at_hour,
        DATE_FORMAT(student_enrolled_on, 'yyyy-MM') AS student_enrolled_on_label
    FROM ranked
    WHERE days_since_prev_student_enrolled_on >= 30
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    course_credits,
    running_course_credits,
    course_credits_vs_course_dept_avg,
    course_credits_sum_3_prior,
    days_since_prev_student_enrolled_on,
    course_credits_change,
    student_enrolled_on_percent_rank,
    rn,
    student_enrolled_on_month_course_id_avg_course_c,
    max_course_credits_so_far,
    course_credits_share_of_course_dept,
    distinct_grade_status_in_course_dept,
    next3_course_credits,
    running_course_credits_distance,
    running_course_credits + course_credits_change AS running_course_credits_plus_course_credits_chang
FROM derived
WHERE course_credits_vs_course_dept_avg >= -2.0
ORDER BY next3_course_credits, course_id DESC
