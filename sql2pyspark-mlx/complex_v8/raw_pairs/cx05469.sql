WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE term = '2024T1'
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_group,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM base_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id AND e.term = '2024T2'
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_group,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_grade_status,
        s.enrollment_student_id,
        s.enrollment_term,
        c.credits AS course_credits,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort
    FROM line_level s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.enrollment_student_id
),
stats AS (
    -- Summarise per group
    SELECT
        course_credits,
        grade_status_group,
        SUM(course_credits) AS total_course_credits,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(course_credits, 0.5) AS median_course_credits,
        COUNT(*) AS n_rows
    FROM enriched_course
    WHERE enrollment_term = '2024T2'
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(distinct_ids) OVER w_n_rows_by_median_course_cred AS next_distinct_ids,
        SUM(n_rows) OVER (PARTITION BY n_rows ORDER BY median_course_credits DESC, course_credits, grade_status_group ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS n_rows_sum_3_prior,
        MIN(total_course_credits) OVER w_distinct_ids_grade_status_gr AS distinct_ids_grade_status_group_min_total_course,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY distinct_ids, grade_status_group), 2) AS course_credits_vs_distinct_ids_grade_status_grou,
        COUNT(*) OVER w_distinct_ids_grade_status_gr AS rows_in_distinct_ids_grade_status_group,
        NTH_VALUE(total_course_credits, 2) OVER (PARTITION BY distinct_ids, grade_status_group ORDER BY median_course_credits ASC NULLS LAST, course_credits, grade_status_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_course_credits
    FROM stats
    WINDOW w_n_rows_by_median_course_cred AS (PARTITION BY n_rows ORDER BY median_course_credits DESC, course_credits, grade_status_group), w_distinct_ids_grade_status_gr AS (PARTITION BY distinct_ids, grade_status_group)
)
-- Final projection
SELECT
    course_credits,
    total_course_credits,
    student_cohort_list,
    distinct_ids,
    median_course_credits,
    n_rows,
    next_distinct_ids,
    n_rows_sum_3_prior,
    distinct_ids_grade_status_group_min_total_course,
    course_credits_vs_distinct_ids_grade_status_grou,
    rows_in_distinct_ids_grade_status_group,
    second_total_course_credits
FROM ranked
WHERE median_course_credits >= 2
