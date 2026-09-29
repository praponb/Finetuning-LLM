WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(session_end) AS session_end_year,
        COALESCE(session_csat, 0) AS session_csat_filled,
        TO_DATE(session_start) AS session_start_date
    FROM session_summary
),
stats AS (
    -- Summarise per group
    SELECT
        YEAR(session_start_date) AS session_start_date_year,
        TO_DATE(session_start) AS session_start_day,
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_ym,
        ROUND(STDDEV(session_csat_filled), 2) AS stddev_session_csat_filled,
        MIN(session_start) AS first_session_start,
        MAX(session_start) AS last_session_start,
        MIN(session_end) AS first_session_end
    FROM annotated
    GROUP BY YEAR(session_start_date), TO_DATE(session_start), DATE_FORMAT(session_end, 'yyyy-MM')
    HAVING SUM(session_end_year) > 2024
)
-- Final projection
SELECT
    session_start_date_year,
    session_start_day,
    session_end_ym,
    first_session_start,
    last_session_start,
    first_session_end,
    DATE_TRUNC('DAY', first_session_start) AS first_session_start_day
FROM stats
ORDER BY last_session_start DESC NULLS FIRST
