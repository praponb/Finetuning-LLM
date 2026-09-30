WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-25 13:00:00'
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        student_id,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_no
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM per_session s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
        )
      AND TO_DATE(s.session_start) < DATE '2025-02-21'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        student_id,
        session_no,
        session_start,
        session_end,
        ROUND(PERCENT_RANK() OVER (ORDER BY session_end), 3) AS session_end_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_count ORDER BY session_start, student_id, session_no), 3) AS session_start_cume_dist,
        FIRST_VALUE(session_end) OVER (ORDER BY session_end, student_id, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_end,
        DENSE_RANK() OVER (PARTITION BY event_count ORDER BY session_start, student_id, session_no) AS session_start_dense_rank
    FROM without_matches
)
SELECT
    student_id,
    session_start,
    first_session_end,
    session_start_dense_rank
FROM metrics
WHERE session_no < 5
ORDER BY first_session_end
