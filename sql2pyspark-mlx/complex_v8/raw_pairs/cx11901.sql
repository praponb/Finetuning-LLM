WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality = 'OK'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY quality_or_unknown, site_id ORDER BY read_at, id) AS read_at_rank,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality_or_unknown), 2) AS kwh_vs_quality_or_unknown_avg
    FROM cleaned_readings
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality_prefix,
        s.read_at_label,
        s.read_at_rank,
        s.kwh_vs_quality_or_unknown_avg,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at_rank, site_commissioned_on, site_zone, kwh_vs_quality_or_unknown_avg
    FROM combined_details
) src
PIVOT (
    MIN(kwh_vs_quality_or_unknown_avg)
    FOR site_zone IN ('Z4', 'Z3', 'Z2')
)
ORDER BY read_at_rank, site_commissioned_on
