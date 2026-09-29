WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 14:00:00'
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM valid_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(session_start, 'yyyy-MM') AS session_start_ym,
        ROUND(100 * COUNT(CASE WHEN quality = 'OK' THEN 1 END) / COUNT(*), 1) AS pct_quality_ok,
        COUNT(*) AS n_rows,
        SUM(n_events) AS total_n_events,
        SUM(CASE WHEN quality <> 'BAD' THEN 1 ELSE 0 END) AS n_quality_bad
    FROM per_session
    WHERE n_events < 5
    GROUP BY DATE_FORMAT(session_start, 'yyyy-MM')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(n_quality_bad) IGNORE NULLS OVER (PARTITION BY n_quality_bad ORDER BY pct_quality_ok, session_start_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_quality_bad,
        ROUND(PERCENT_RANK() OVER w_n_quality_bad_n_rows_by_n_ro, 3) AS n_rows_percent_rank
    FROM totals
    WINDOW w_n_quality_bad_n_rows_by_n_ro AS (PARTITION BY n_quality_bad, n_rows ORDER BY n_rows DESC)
)
SELECT session_start_ym, metric, value
FROM (
    SELECT session_start_ym, total_n_events, first_known_n_quality_bad, n_quality_bad, n_rows
    FROM with_trends
) src
UNPIVOT (
    value FOR metric IN (total_n_events, first_known_n_quality_bad, n_quality_bad, n_rows)
) unp
ORDER BY session_start_ym, metric
