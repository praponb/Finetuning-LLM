WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term IN ('2024T1', '2024T2')
              AND HOUR(e.enrolled_at) BETWEEN 6 AND 12
        )
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.prev_enrolled_at,
        s.starts_session,
        s.session_no,
        c.dept AS course_dept,
        st.cohort AS student_cohort
    FROM filtered s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
    WHERE c.dept = 'Biz'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, starts_session, session_no
    FROM with_details_course
) src
UNPIVOT (
    value FOR metric_name IN (starts_session, session_no)
) unp
ORDER BY id, metric_name
