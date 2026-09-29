WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(session_end) OVER (PARTITION BY event_count ORDER BY duration_min, grade_status, session_id) AS next_session_end,
        RANK() OVER (PARTITION BY event_count ORDER BY duration_min, grade_status, session_id) AS duration_min_rank,
        LEAD(session_id, 2) OVER (PARTITION BY event_count ORDER BY duration_min, grade_status, session_id) AS next2_session_id,
        ROW_NUMBER() OVER (PARTITION BY event_count ORDER BY duration_min, grade_status, session_id) AS rn,
        LAST_VALUE(session_start) IGNORE NULLS OVER (PARTITION BY event_count ORDER BY duration_min, grade_status, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start,
        ROUND(duration_min / NULLIF(SUM(duration_min) OVER w_grade_status_session_id, 0), 4) AS duration_min_share_of_grade_status_session_id
    FROM session_summary
    WINDOW w_grade_status_session_id AS (PARTITION BY grade_status, session_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY session_id, DATE_TRUNC('MONTH', last_known_session_start) ORDER BY event_count DESC) AS event_count_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', session_end)) AS rows_in_session_end_month,
        ROUND(AVG(duration_min) OVER (PARTITION BY session_id, DATE_TRUNC('MONTH', last_known_session_start) ORDER BY event_count DESC, grade_status, session_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_min_avg_2_prior,
        MAX(duration_min) OVER w_session_end_month AS session_end_month_max_duration_min
    FROM with_trends
    WINDOW w_session_end_month AS (PARTITION BY DATE_TRUNC('MONTH', session_end))
)
-- Final projection
SELECT
    grade_status,
    session_id,
    session_start,
    event_count,
    duration_min,
    next_session_end,
    duration_min_rank,
    rn,
    last_known_session_start,
    duration_min_share_of_grade_status_session_id,
    event_count_rank,
    rows_in_session_end_month,
    duration_min_avg_2_prior
FROM sequenced
ORDER BY duration_min_share_of_grade_status_session_id DESC, session_start
