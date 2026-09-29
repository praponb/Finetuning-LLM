WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM filtered
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
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        voltage,
        quality,
        voltage_bucket,
        prev_read_at,
        is_new_session,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY TO_DATE(read_at), is_new_session), 0), 4) AS voltage_share_of_read_at_to_date_is_new_session,
        SIZE(COLLECT_SET(quality) OVER w_read_at_to_date_is_new_sessi) AS distinct_quality_in_read_at_to_date_is_new_sessi
    FROM sessionized
    WINDOW w_read_at_to_date_is_new_sessi AS (PARTITION BY TO_DATE(read_at), is_new_session)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(voltage_bucket / NULLIF(voltage, 0), 3) AS voltage_bucket_per_voltage
    FROM with_windows
    WHERE is_new_session < 1
)
SELECT *
FROM (
    SELECT prev_read_at, quality, voltage_bucket
    FROM scored
) src
PIVOT (
    COUNT(voltage_bucket)
    FOR quality IN ('EST', 'OK')
)
