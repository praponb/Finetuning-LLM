WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        voltage - LAG(voltage) OVER (PARTITION BY is_new_session ORDER BY kwh DESC NULLS FIRST, id) AS voltage_change,
        MAX(burst_id) OVER (PARTITION BY is_new_session ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS burst_id_max_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at ASC), 3) AS read_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY is_new_session ORDER BY kwh DESC NULLS FIRST) AS kwh_dense_rank,
        MIN(burst_id) OVER (PARTITION BY is_new_session) AS is_new_session_min_burst_id
    FROM sessionized
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    is_new_session,
    voltage_change,
    read_at_percent_rank,
    kwh_dense_rank,
    CONCAT(quality, '#', CAST(burst_id_max_4 AS STRING)) AS quality_ref,
    COALESCE(read_at_percent_rank, 0.0) AS read_at_percent_rank_filled
FROM with_trends
WHERE MONTH(read_at) IN (1, 3)
ORDER BY voltage_change DESC NULLS FIRST, kwh NULLS LAST
