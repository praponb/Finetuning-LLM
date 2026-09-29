WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE grade_status NOT IN ('PASS', 'INCOMPLETE')
      AND course_id <= 2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        c.dept AS course_dept,
        c.level AS course_level,
        c.course_code,
        st.cohort AS student_cohort,
        st.major AS student_major
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM enriched s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_count, course_dept, score
    FROM summarized_rows
) src
PIVOT (
    MAX(score)
    FOR course_dept IN ('Math', 'Art', 'Bio', 'Biz')
)
