WITH cleaned AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        site_id,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        site_id,
        session_no,
        session_start,
        session_end,
        events,
        session_kwh,
        COUNT(*) OVER (PARTITION BY YEAR(session_end)) AS rows_in_session_end_year,
        SUM(session_no) OVER (ORDER BY duration_min DESC, site_id, session_no ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS session_no_sum_1_prior
    FROM session_summary
)
SELECT
    session_no,
    site_id,
    GROUPING(session_no) AS is_total_session_no,
    MAX(rows_in_session_end_year) AS max_rows_in_session_end_year,
    COUNT(*) AS n_rows
FROM sequenced
GROUP BY GROUPING SETS ((session_no, site_id), (session_no), ())
