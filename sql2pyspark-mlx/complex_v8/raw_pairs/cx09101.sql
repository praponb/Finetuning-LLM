WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE score < 87.8
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at_quarter,
        c.dept AS course_dept,
        c.course_code,
        c.level AS course_level,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM cleaned s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
totals AS (
    -- Group-level totals
    SELECT
        course_dept,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        SUM(CASE WHEN course_dept = 'CS' THEN 1 ELSE 0 END) AS n_course_dept_cs,
        COUNT(*) AS row_count
    FROM with_details_course
    WHERE course_id IS NOT NULL
    GROUP BY 1
),
enriched AS (
    -- Derive helper columns
    SELECT
        course_dept,
        student_cohort_list,
        n_course_dept_cs,
        row_count,
        CASE WHEN course_dept IN ('Bio', 'Math') THEN 'group_a' WHEN course_dept = 'Biz' THEN 'group_b' ELSE 'other' END AS course_dept_group
    FROM totals
    WHERE n_course_dept_cs BETWEEN 3 AND 7
)
-- Final projection
SELECT
    course_dept,
    student_cohort_list,
    n_course_dept_cs,
    row_count,
    course_dept_group,
    SUBSTRING(TRIM(course_dept_group), 1, 2) AS course_dept_group_prefix,
    CONCAT_WS(': ', course_dept, course_dept_group) AS course_dept_course_dept_group_label
FROM enriched
ORDER BY course_dept, row_count DESC
