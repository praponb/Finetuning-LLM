WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE kwh > 20.4
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY session_no, site_id ORDER BY read_at DESC) AS read_at_dense_rank,
        MAX(new_session_flag) OVER (PARTITION BY session_no, site_id ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS new_session_flag_max_2
    FROM sessionized
)
SELECT
    id,
    kwh,
    voltage,
    voltage_minus_kwh,
    prev_read_at,
    new_session_flag,
    session_no,
    read_at_dense_rank,
    new_session_flag_max_2
FROM with_trends
ORDER BY voltage
