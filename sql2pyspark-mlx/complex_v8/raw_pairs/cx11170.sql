WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE (kwh BETWEEN 20.3 AND 83.8 OR voltage <= 235.0)
),
enriched AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM scoped_readings
    WHERE site_id >= 2
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'EST'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT *, ROUND(voltage * 0.9, 2) AS voltage_adj
    FROM qualified
    WHERE kwh_per_voltage <= 7.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_read_at_month_by_voltage, 3) AS voltage_percent_rank,
        DENSE_RANK() OVER (ORDER BY voltage_adj ASC) AS voltage_adj_dense_rank
    FROM tagged
    WINDOW w_read_at_month_by_voltage AS (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY voltage ASC, id)
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh_per_voltage,
        s.voltage_adj,
        s.voltage_adj_dense_rank,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM ranked s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh_per_voltage,
        s.voltage_adj_dense_rank,
        s.reading_id,
        s.reading_read_at,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM exploded_detail_readings s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    read_at,
    voltage_adj_dense_rank,
    reading_id,
    reading_read_at,
    site_zone,
    site_commissioned_on,
    site_tariff
FROM combined_details_site
WHERE kwh_per_voltage <= 8.0
ORDER BY site_zone DESC
