WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        grade_status,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_no
),
tagged AS (
    -- Derive helper columns
    SELECT
        grade_status,
        session_no,
        session_start,
        session_end,
        session_score,
        duration_min,
        TO_DATE(session_end) AS session_end_date
    FROM per_session
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        grade_status,
        session_no,
        session_end,
        session_score,
        duration_min,
        ROUND(AVG(duration_min) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', session_end_date) ORDER BY duration_min DESC, grade_status, session_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_min_avg_4,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        MAX(session_no) OVER (PARTITION BY grade_status, session_no ORDER BY session_end_date, grade_status, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_session_no_so_far,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY grade_status), 0), 4) AS session_no_share_of_grade_status,
        ROUND(session_no - AVG(session_no) OVER (PARTITION BY grade_status), 2) AS session_no_vs_grade_status_avg
    FROM tagged
)
SELECT
    grade_status,
    session_no,
    session_end,
    session_score,
    duration_min,
    duration_min_avg_4,
    rows_in_grade_status,
    max_session_no_so_far,
    session_no_vs_grade_status_avg,
    ROUND(rows_in_grade_status / NULLIF(session_no, 0), 3) AS rows_in_grade_status_per_session_no
FROM with_trends
WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN')
