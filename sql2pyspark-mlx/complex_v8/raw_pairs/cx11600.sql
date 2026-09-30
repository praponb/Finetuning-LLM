WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-01-19 09:00:00'
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(voltage) OVER w_site_id_read_at_to_date AS site_id_read_at_to_date_sum_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, starts_session ORDER BY read_at), 3) AS read_at_percent_rank
    FROM with_activity
    WINDOW w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at))
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    prev_read_at,
    starts_session,
    burst_id,
    site_id_read_at_to_date_sum_voltage,
    read_at_percent_rank,
    ABS(voltage - 220.7) AS voltage_distance
FROM windowed
WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
ORDER BY prev_read_at
