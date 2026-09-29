WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
enriched_course AS (
    -- Attach course attributes
    SELECT s.*, c.course_code, st.enrolled_on AS student_enrolled_on
    FROM prepared_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    term,
    GROUPING(course_id) AS is_total_course_id,
    ROUND(STDDEV(score), 2) AS stddev_score
FROM enriched_course
GROUP BY ROLLUP(course_id, term)
ORDER BY course_id NULLS LAST, term NULLS LAST
