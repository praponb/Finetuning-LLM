WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest_per_key AS (
    -- Keep the earliest row per term, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        prev_enrolled_at,
        starts_session,
        session_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, student_id ORDER BY starts_session, id) AS rn
        FROM sessionized
    ) ranked
    WHERE rn = 1
),
student_id_stats AS (
    -- Max session_id per student_id
    SELECT student_id, MAX(session_id) AS max_session_id
    FROM latest_per_key
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_session_id,
        ROUND(s.session_id - g.max_session_id, 2) AS session_id_minus_max
    FROM latest_per_key s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.session_id = g.max_session_id
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    starts_session,
    session_id,
    max_session_id,
    ABS(starts_session - 0) AS starts_session_distance,
    HOUR(prev_enrolled_at) AS prev_enrolled_at_hour
FROM above_average
ORDER BY max_session_id, student_id
