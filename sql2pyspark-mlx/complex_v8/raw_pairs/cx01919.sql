WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(voltage_filled) IGNORE NULLS OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_filled,
        SUM(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_sum_1_prior,
        DENSE_RANK() OVER (PARTITION BY site_id, quality ORDER BY kwh ASC, id) AS kwh_dense_rank
    FROM prepared
),
with_details_site AS (
    -- Attach site attributes
    SELECT s.*, si.tariff AS site_tariff
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(site_tariff, '#', CAST(kwh_dense_rank AS STRING)) AS site_tariff_ref
    FROM with_details_site
)
-- Subtotals with GROUPING SETS
SELECT
    site_tariff,
    site_id,
    quality,
    GROUPING(site_tariff) AS is_total_site_tariff,
    COUNT(*) AS cnt,
    COUNT(DISTINCT quality) AS distinct_qualitys,
    COUNT(DISTINCT site_tariff) AS distinct_site_tariffs
FROM labeled
GROUP BY GROUPING SETS ((site_tariff, site_id, quality), (site_tariff), (site_id))
ORDER BY site_tariff NULLS LAST, site_id NULLS LAST, quality NULLS LAST
