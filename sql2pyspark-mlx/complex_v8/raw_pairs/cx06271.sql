WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 220.2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.site_name,
        s.site_commissioned_on,
        s.site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on_2
    FROM enriched s
    LEFT JOIN sites si ON si.id = s.site_id
),
site_tariff_stats AS (
    -- Median kwh per site_tariff
    SELECT site_tariff, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM combined_details
    GROUP BY site_tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM combined_details s
    JOIN site_tariff_stats g ON g.site_tariff = s.site_tariff
    WHERE s.kwh >= g.median_kwh
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        kwh,
        site_name,
        site_commissioned_on,
        site_tariff,
        site_commissioned_on_2,
        median_kwh,
        kwh_minus_median,
        DATEDIFF(DATE '2025-06-30', site_commissioned_on_2) AS days_since_site_commissioned_on_2,
        ROUND(kwh / NULLIF(median_kwh, 0), 3) AS kwh_per_median_kwh,
        GREATEST(median_kwh, kwh_minus_median) AS greatest_median_kwh_kwh_minus_median
    FROM above_average
    WHERE UPPER(TRIM(site_name)) LIKE 'M%'
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    site_name,
    site_commissioned_on,
    site_tariff,
    site_commissioned_on_2,
    kwh_minus_median,
    days_since_site_commissioned_on_2,
    kwh_per_median_kwh,
    greatest_median_kwh_kwh_minus_median,
    ABS(kwh_minus_median - 36.0) AS kwh_minus_median_distance,
    ROUND(median_kwh * 0.25, 2) AS median_kwh_adj
FROM labeled
WHERE days_since_site_commissioned_on_2 < 420
ORDER BY site_tariff DESC
