WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 81.4 THEN 'HIGH' WHEN score >= 38.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        course_id,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, session_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(session_id) OVER w_course_id_session_id AS course_id_session_id_sum_session_id,
        COUNT(*) OVER (PARTITION BY course_id, session_id) AS rows_in_course_id_session_id,
        LEAD(n_events) OVER w_session_end_to_date_by_sessi AS next_n_events,
        SUM(session_score) OVER (PARTITION BY course_id, session_id) AS course_id_session_id_sum_session_score,
        LAG(session_end, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', session_start), course_id ORDER BY session_end DESC, course_id, session_id) AS prev2_session_end
    FROM sessions
    WINDOW w_course_id_session_id AS (PARTITION BY course_id, session_id), w_session_end_to_date_by_sessi AS (PARTITION BY TO_DATE(session_end) ORDER BY session_start, course_id, session_id)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MIN(enrolled_at) AS first_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.first_enrollment_enrolled_at,
        e.first_enrollment_enrolled_at_2
    FROM sequenced s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    course_id,
    session_start,
    duration_min,
    course_id_session_id_sum_session_id,
    rows_in_course_id_session_id,
    next_n_events,
    course_id_session_id_sum_session_score,
    enrollment_count,
    first_enrollment_enrolled_at_2,
    TO_DATE(session_end) AS session_end_date
FROM summarized_rows
ORDER BY enrollment_count
