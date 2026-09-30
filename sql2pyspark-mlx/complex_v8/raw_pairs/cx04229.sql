WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 14
),
tagged AS (
    -- Label and score rows
    SELECT *, ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM recent_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 11
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.kwh_adj,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM tagged s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT read_at, site_id, site_zone, kwh
    FROM with_details
) src
PIVOT (
    COUNT(kwh) AS count_v, SUM(kwh) AS sum_v
    FOR site_zone IN ('Z4', 'Z3', 'Z2')
)
ORDER BY read_at, site_id
