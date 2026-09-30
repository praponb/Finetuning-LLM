WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN voltage >= 235.0 THEN 'large' WHEN voltage >= 223.3 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    voltage_tier,
    prev_read_at,
    session_id,
    DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
FROM sessionized
WHERE TO_DATE(read_at) < DATE '2025-03-02'
ORDER BY kwh
