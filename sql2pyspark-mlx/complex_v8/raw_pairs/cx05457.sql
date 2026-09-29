WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        course_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, burst_id
),
with_details_course AS (
    -- Attach course attributes
    SELECT s.*, c.course_code, c.dept AS course_dept
    FROM per_session s
    JOIN courses c ON c.id = s.course_id
),
filtered_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_details_course s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id IS NOT NULL
              AND TO_DATE(e.enrolled_at) < DATE '2025-03-08'
        )
)
SELECT
    course_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    course_code,
    YEAR(session_end) AS session_end_year,
    LENGTH(TRIM(course_code)) AS course_code_len
FROM filtered_2
ORDER BY n_events DESC, duration_min DESC
