WITH valid_enrollments AS (
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
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        student_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, burst_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        student_id,
        burst_id,
        session_start,
        session_end,
        duration_min,
        COUNT(*) OVER (PARTITION BY burst_id ORDER BY duration_min, student_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(3) OVER (ORDER BY duration_min DESC, student_id, burst_id) AS duration_min_tercile
    FROM sessions
)
-- Native SQL UNPIVOT (wide to long)
SELECT student_id, burst_id, field, metric_value
FROM (
    SELECT student_id, burst_id, running_count, duration_min_tercile
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR field IN (running_count, duration_min_tercile)
) unp
