WITH base AS (
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
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        term,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_no
),
per_group AS (
    -- Group-level totals
    SELECT
        event_count,
        session_no,
        COUNT(*) AS cnt,
        SUM(event_count) AS total_event_count,
        COUNT(DISTINCT term) AS distinct_terms,
        ROUND(AVG(event_count), 2) AS avg_event_count,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM sessions
    GROUP BY 1, 2
    HAVING SUM(duration_min) > 72.0
),
scored AS (
    -- Label and score rows
    SELECT *, ROUND(avg_event_count * 1.07, 2) AS avg_event_count_adj
    FROM per_group
)
-- Subtotals with ROLLUP
SELECT
    cnt,
    session_no,
    GROUPING(cnt) AS is_total_cnt,
    PERCENTILE_APPROX(avg_event_count_adj, 0.5) AS median_avg_event_count_adj,
    MAX(avg_event_count) AS max_avg_event_count
FROM scored
GROUP BY ROLLUP(cnt, session_no)
