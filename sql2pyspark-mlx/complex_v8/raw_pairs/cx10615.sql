WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN term IN ('2025T2', '2025T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE student_id >= 16
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.term_group,
        s.grade_status_clean,
        s.term_clean,
        c.dept AS course_dept,
        c.course_code
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    course_dept,
    grade_status,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined
GROUP BY CUBE(course_dept, grade_status)
ORDER BY course_dept NULLS LAST, grade_status NULLS LAST
