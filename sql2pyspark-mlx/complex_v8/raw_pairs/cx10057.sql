WITH recent_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent_sites s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.read_at < TIMESTAMP '2025-02-18 13:00:00'
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(site_name)) AS site_name_len,
        CASE WHEN tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM without_matches
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    tariff,
    commissioned_on,
    site_name_len,
    CONCAT_WS('|', site_name, zone) AS site_name_zone_label
FROM annotated
ORDER BY tariff NULLS LAST, site_name, id
LIMIT 10
