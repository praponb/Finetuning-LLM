WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 6 AND 10
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_clean,
        s.prev_enrolled_at,
        c.dept AS course_dept,
        st.is_international AS student_is_international
    FROM sessionized s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
)
-- Subtotals with ROLLUP
SELECT
    course_dept,
    course_id,
    student_is_international,
    COUNT(*) AS row_count
FROM combined_details
GROUP BY ROLLUP(course_dept, course_id, student_is_international)
