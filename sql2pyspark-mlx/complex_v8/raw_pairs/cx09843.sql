WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 3)
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        student_id,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_id
),
tagged AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(session_end, 'MMM yyyy') AS session_end_label
    FROM session_summary
)
SELECT
    student_id,
    session_id,
    session_start,
    events,
    session_end_label
FROM tagged
ORDER BY session_start DESC
