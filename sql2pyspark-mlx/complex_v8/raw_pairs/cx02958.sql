WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 15 MINUTES AS read_at_plus_15m,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE voltage < 236.7
      AND site_id >= 7
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        kwh,
        quality,
        read_at_plus_15m,
        read_at_date,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        DATE_FORMAT(read_at_plus_15m, 'yyyy-MM') AS read_at_plus_15m_label,
        ROUND(voltage * 1.5, 2) AS voltage_adj
    FROM filtered_readings
)
SELECT id, metric, reading
FROM (
    SELECT id, kwh, voltage_adj
    FROM derived
) src
UNPIVOT (
    reading FOR metric IN (kwh, voltage_adj)
) unp
ORDER BY id, metric
