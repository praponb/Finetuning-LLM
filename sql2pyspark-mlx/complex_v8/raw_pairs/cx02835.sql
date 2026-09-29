WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        grade_status,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_no
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(events) OVER (PARTITION BY MONTH(session_end) ORDER BY session_start ASC, grade_status, session_no ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS events_max_1_prior,
        COUNT(*) OVER w_session_start_month_session AS rows_in_session_start_month_session_no,
        ROUND(PERCENT_RANK() OVER w_session_end_month_by_session_2, 3) AS session_start_percent_rank,
        MIN(duration_min) OVER (PARTITION BY DATE_TRUNC('MONTH', session_start), session_no) AS session_start_month_session_no_min_duration_min,
        SIZE(COLLECT_SET(grade_status) OVER w_session_start_month_session) AS distinct_grade_status_in_session_start_month_ses
    FROM session_summary
    WINDOW w_session_start_month_session AS (PARTITION BY DATE_TRUNC('MONTH', session_start), session_no), w_session_end_month_by_session_2 AS (PARTITION BY MONTH(session_end) ORDER BY session_start ASC)
)
SELECT *
FROM (
    SELECT session_no, session_end, grade_status, distinct_grade_status_in_session_start_month_ses
    FROM with_trends
) src
PIVOT (
    AVG(distinct_grade_status_in_session_start_month_ses) AS avg_v, SUM(distinct_grade_status_in_session_start_month_ses) AS sum_v
    FOR grade_status IN ('INCOMPLETE', 'FAIL')
)
