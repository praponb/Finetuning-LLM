WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2024T2'
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
grade_status_stats AS (
    -- Max new_session_flag per grade_status
    SELECT grade_status, MAX(new_session_flag) AS max_new_session_flag
    FROM sessionized
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_new_session_flag,
        ROUND(s.new_session_flag - g.max_new_session_flag, 2) AS new_session_flag_minus_max
    FROM sessionized s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.new_session_flag = g.max_new_session_flag
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM outliers s
)
-- Subtotals with GROUPING SETS
SELECT
    burst_id,
    course_id,
    enrollment_count,
    COUNT(*) FILTER (WHERE grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE')) AS n_grade_status_in_pass_withdra
FROM profiled
GROUP BY GROUPING SETS ((burst_id, course_id, enrollment_count), (burst_id), (course_id), ())
