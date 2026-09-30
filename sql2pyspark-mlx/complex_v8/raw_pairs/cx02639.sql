WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE score < 87.8
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.course_id <= 2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.enrolled_on AS student_enrolled_on,
        st.student_name,
        st.gpa AS student_gpa,
        c.course_code,
        c.dept AS course_dept
    FROM without_matches s
    JOIN students st ON st.id = s.student_id AND st.major NOT IN ('Biz', 'Math')
    JOIN courses c ON c.id = s.course_id
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at_week,
        s.student_enrolled_on,
        s.student_gpa,
        s.course_code,
        s.course_dept,
        c.dept AS course_dept_2
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept IN ('Math', 'CS')
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, student_gpa, score
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (student_gpa, score)
) unp
