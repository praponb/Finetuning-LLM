WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh_per_voltage,
        prev_read_at,
        session_id,
        GREATEST(voltage, session_id) AS greatest_voltage_session_id,
        LENGTH(TRIM(quality)) AS quality_len,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM sessionized
    WHERE voltage < 231.7
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ROUND(SUM(session_id) / NULLIF(SUM(greatest_voltage_session_id), 0), 3) AS session_id_to_greatest_voltage_session_id_ratio,
        MIN(session_id) AS min_session_id,
        MIN(kwh_per_voltage) AS min_kwh_per_voltage,
        COUNT(*) FILTER (WHERE quality = 'EST') AS n_quality_est,
        COUNT(*) AS n_rows
    FROM annotated
    GROUP BY site_id, DATE_TRUNC('MONTH', read_at)
)
-- Native SQL UNPIVOT (wide to long)
SELECT site_id, read_at_month, measure, reading
FROM (
    SELECT site_id, read_at_month, min_kwh_per_voltage, session_id_to_greatest_voltage_session_id_ratio
    FROM totals
) src
UNPIVOT (
    reading FOR measure IN (min_kwh_per_voltage AS m_min_kwh_per_voltage, session_id_to_greatest_voltage_session_id_ratio AS session_id_to_greatest_voltage_session_id_ratio_)
) unp
