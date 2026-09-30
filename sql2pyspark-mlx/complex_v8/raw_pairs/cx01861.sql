WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        prev_read_at,
        is_new_session,
        burst_id,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        DATE_TRUNC('MONTH', prev_read_at) AS prev_read_at_month,
        GREATEST(burst_id, kwh) AS greatest_burst_id_kwh
    FROM sessionized
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    prev_read_at,
    is_new_session,
    burst_id,
    quality_prefix,
    prev_read_at_month,
    greatest_burst_id_kwh,
    INITCAP(TRIM(quality_prefix)) AS quality_prefix_clean,
    kwh - voltage AS kwh_minus_voltage
FROM tagged
