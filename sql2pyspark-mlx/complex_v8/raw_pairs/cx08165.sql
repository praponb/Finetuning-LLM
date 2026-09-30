WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        ABS(voltage - 222.5) AS voltage_distance
    FROM meter_readings
    WHERE voltage <= 234.5
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 6 AND 10
              AND m.quality NOT IN ('OK', 'BAD')
        )
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on >= DATE '2024-09-15'
        )
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality_prefix,
        s.days_since_read_at,
        s.voltage_distance,
        si.site_name
    FROM screened s
    JOIN sites si ON si.id = s.site_id
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        kwh,
        quality_prefix,
        days_since_read_at,
        voltage_distance,
        site_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY days_since_read_at DESC, id DESC) AS recency_rank
        FROM joined
    ) ranked
    WHERE recency_rank = 1
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality_prefix,
        s.voltage_distance,
        s.site_name,
        si.zone AS site_zone,
        si.site_name AS site_site_name
    FROM latest s
    JOIN sites si ON si.id = s.site_id AND si.zone = 'Z4'
)
SELECT id, measure, metric_value
FROM (
    SELECT id, kwh, voltage_distance
    FROM with_details_site
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (kwh, voltage_distance)
) unp
ORDER BY id, measure
