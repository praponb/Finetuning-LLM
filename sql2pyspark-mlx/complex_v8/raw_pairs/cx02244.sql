WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE grade_status <> 'WITHDRAWN'
      AND course_id IS NOT NULL
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at_label,
        s.enrolled_at_quarter,
        s.grade_status_group,
        st.is_international AS student_is_international
    FROM prepared s
    LEFT JOIN students st ON st.id = s.student_id AND UPPER(TRIM(st.student_name)) LIKE 'J%'
)
-- Subtotals with ROLLUP
SELECT
    student_is_international,
    grade_status_group,
    GROUPING(student_is_international) AS is_total_student_is_international,
    SUM(enrolled_at_quarter) AS total_enrolled_at_quarter,
    COUNT(*) AS cnt
FROM with_parent
GROUP BY ROLLUP(student_is_international, grade_status_group)
ORDER BY student_is_international NULLS LAST, grade_status_group NULLS LAST
