WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'INCOMPLETE')
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        student_id,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(term_len) AS session_term_len,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_no
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(LAG(session_end) OVER (PARTITION BY MONTH(session_end), session_no ORDER BY session_term_len DESC, student_id, session_no))) / 60, 1) AS minutes_since_prev_session_end,
        ROUND(session_no - AVG(session_no) OVER (PARTITION BY student_id), 2) AS session_no_vs_student_id_avg,
        ROW_NUMBER() OVER (ORDER BY n_events, student_id, session_no) AS n_events_seq,
        FIRST_VALUE(session_term_len) OVER (ORDER BY duration_min DESC, student_id, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_term_len,
        SUM(n_events) OVER w_student_id_2 AS student_id_sum_n_events
    FROM session_summary
    WINDOW w_student_id_2 AS (PARTITION BY student_id)
)
SELECT
    student_id,
    session_no,
    session_start,
    session_end,
    n_events,
    minutes_since_prev_session_end,
    session_no_vs_student_id_avg,
    first_session_term_len,
    DATEDIFF(DATE '2025-06-30', session_end) AS days_since_session_end,
    ROUND(n_events / NULLIF(n_events_seq, 0), 3) AS n_events_per_n_events_seq
FROM metrics
ORDER BY session_end DESC, student_id, session_no
LIMIT 10
