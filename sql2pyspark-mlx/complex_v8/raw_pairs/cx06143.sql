WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        ABS(kwh - 81.9) AS kwh_distance,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM recent_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw > 141.6
              AND si.tariff IS NOT NULL
        )
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.read_at_year,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM filtered s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw < 284.0
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.site_name,
        s.site_commissioned_on,
        si.tariff AS site_tariff
    FROM joined_site s
    JOIN sites si ON si.id = s.site_id
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.site_name,
        s.site_tariff,
        si.commissioned_on AS site_commissioned_on_2,
        si.site_name AS site_site_name
    FROM enriched_site s
    JOIN sites si ON si.id = s.site_id AND si.tariff = 'TOU'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        quality,
        kwh_distance,
        site_name,
        site_tariff,
        site_commissioned_on_2,
        LEAD(kwh_distance, 3, 0) OVER (ORDER BY site_commissioned_on_2 DESC NULLS FIRST, id) AS next3_kwh_distance,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage
    FROM joined
)
SELECT
    id,
    site_id,
    voltage,
    quality,
    kwh_distance,
    site_name,
    site_commissioned_on_2,
    next3_kwh_distance,
    site_id_min_voltage,
    COALESCE(voltage, 0.0) AS voltage_filled
FROM metrics
WHERE site_id IS NOT NULL
