WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND score <= 77.0
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_group,
        c.dept AS course_dept,
        st.enrolled_on AS student_enrolled_on
    FROM prepared s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 16
              AND e.score >= 42.4
        )
      AND s.student_id >= 19
),
tagged AS (
    -- Derive helper columns
    SELECT *, CONCAT(term_group, '#', CAST(course_id AS STRING)) AS term_group_ref
    FROM filtered
    WHERE student_id <= 9
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    course_dept,
    student_enrolled_on,
    term_group_ref,
    DATEDIFF(DATE '2026-01-01', student_enrolled_on) AS days_since_student_enrolled_on
FROM tagged
WHERE term_group_ref IS NOT NULL
ORDER BY enrolled_at, days_since_student_enrolled_on DESC
