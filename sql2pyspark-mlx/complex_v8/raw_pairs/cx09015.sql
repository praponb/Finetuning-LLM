WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM enrollments
    WHERE grade_status NOT IN ('INCOMPLETE', 'PASS')
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scoped s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.term NOT IN ('2024T1', '2024T2')
              AND e.score >= 42.4
        )
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrolled_at_day,
        c.level AS course_level,
        st.enrolled_on AS student_enrolled_on
    FROM screened s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    course_level,
    course_id,
    MAX(enrolled_at) AS last_enrolled_at,
    SUM(CASE WHEN course_level IS NOT NULL THEN score ELSE 0 END) AS score_course_level_is_not_null
FROM joined_course
GROUP BY GROUPING SETS ((course_level, course_id), (course_level))
ORDER BY course_level NULLS LAST, course_id NULLS LAST
