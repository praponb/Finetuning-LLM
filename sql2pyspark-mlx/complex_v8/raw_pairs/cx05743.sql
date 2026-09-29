WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(new_session_flag - AVG(new_session_flag) OVER (PARTITION BY session_no), 2) AS new_session_flag_vs_session_no_avg,
        COUNT(*) OVER w_session_no AS rows_in_session_no,
        DENSE_RANK() OVER (ORDER BY new_session_flag ASC, id) AS new_session_flag_dense_rank,
        SUM(session_no) OVER (ORDER BY new_session_flag ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS session_no_sum_3_prior,
        LEAD(new_session_flag, 3) OVER (ORDER BY new_session_flag ASC, id) AS next3_new_session_flag,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY session_no)) AS distinct_site_id_in_session_no
    FROM sessionized
    WINDOW w_session_no AS (PARTITION BY session_no)
)
-- Subtotals with CUBE
SELECT
    quality,
    next3_new_session_flag,
    COUNT(DISTINCT site_id) AS distinct_site_ids,
    SUM(new_session_flag_vs_session_no_avg) AS total_new_session_flag_vs_session_no_avg
FROM ranked
GROUP BY CUBE(quality, next3_new_session_flag)
ORDER BY quality NULLS LAST, next3_new_session_flag NULLS LAST
