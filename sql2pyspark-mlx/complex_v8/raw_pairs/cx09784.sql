WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage >= 213.7
      AND quality = 'BAD'
),
joined AS (
    -- Enrich with dimension data
    SELECT s.id, s.site_id, si.zone AS site_zone
    FROM filtered s
    JOIN sites si ON si.id = s.site_id AND si.tariff IN ('TOU', 'FLAT')
    WHERE si.zone = 'Z3'
)
-- Final projection
SELECT id, site_id, site_zone
FROM joined
