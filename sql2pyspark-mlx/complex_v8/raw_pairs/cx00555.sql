WITH prepared_sites AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE tariff = 'TOU'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        QUARTER(commissioned_on) AS commissioned_on_quarter,
        LENGTH(TRIM(site_name)) AS site_name_len,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM prepared_sites
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.commissioned_on_quarter,
        s.site_name_len,
        s.zone_or_unknown,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM scored s
    LEFT JOIN meter_readings m ON m.site_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, measure, amount
FROM (
    SELECT id, reading_id, site_name_len, commissioned_on_quarter
    FROM with_children
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (site_name_len AS site_name_len_v, commissioned_on_quarter AS commissioned_on_quarter_v)
) unp
ORDER BY id, reading_id, measure
