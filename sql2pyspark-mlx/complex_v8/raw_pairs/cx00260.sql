WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
    WHERE voltage < 231.7
)
SELECT quality, id AS ref_id, kwh AS metric
FROM recent
UNION ALL
SELECT tariff, id, installed_kw
FROM sites
UNION ALL
SELECT site_name, id, installed_kw
FROM sites
WHERE commissioned_on < DATE '2025-05-10'
ORDER BY quality, ref_id
