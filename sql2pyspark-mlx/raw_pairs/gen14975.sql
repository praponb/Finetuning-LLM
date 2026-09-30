WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(device_code) clean_device_code
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        rn,
        burst_id
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity NULLS LAST, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        firmware,
        rn,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY burst_id, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) pressure_avg_5_prior,
        ROW_NUMBER() OVER w_recent row_seq
    FROM deduped
    WINDOW w_recent AS (ORDER BY pressure DESC, id)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(rn, row_seq) least_rn_row_seq
    FROM enriched
    WHERE pressure_avg_5_prior BETWEEN 1007 AND 1025
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    SUM(least_rn_row_seq) FILTER (WHERE device_code = 'DV-03') AS dv_03_total_least_rn_row_seq,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows
FROM base
GROUP BY CASE WHEN row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END
HAVING dv_01_rows > 1
ORDER BY dv_01_rows DESC NULLS FIRST
