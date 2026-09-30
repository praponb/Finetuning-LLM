WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE quality = 'BAD'
      AND voltage <= 239.6
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.site_name,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, YEAR(read_at) ORDER BY voltage), 3) AS voltage_percent_rank,
        voltage - LAG(voltage) OVER (ORDER BY site_commissioned_on DESC, id) AS voltage_change,
        FIRST_VALUE(voltage) OVER (PARTITION BY quality, YEAR(read_at) ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        LAG(kwh) OVER (PARTITION BY quality, YEAR(read_at) ORDER BY voltage, id) AS prev_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, YEAR(site_commissioned_on)), 0), 4) AS kwh_share_of_quality_site_commissioned_on_year,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY quality, YEAR(site_commissioned_on))) AS distinct_site_zone_in_quality_site_commissioned_
    FROM combined_details_site
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        quality,
        site_name,
        site_zone,
        site_commissioned_on,
        first_known_kwh,
        voltage_change,
        first_voltage,
        prev_kwh,
        kwh_share_of_quality_site_commissioned_on_year,
        distinct_site_zone_in_quality_site_commissioned_,
        ROUND(first_known_kwh - AVG(first_known_kwh) OVER (PARTITION BY site_zone), 2) AS first_known_kwh_vs_site_zone_avg,
        ROW_NUMBER() OVER (PARTITION BY distinct_site_zone_in_quality_site_commissioned_, site_zone ORDER BY distinct_site_zone_in_quality_site_commissioned_, id) AS distinct_site_zone_in_quality_site_commissioned__2,
        SUM(voltage_change) OVER (PARTITION BY site_zone) AS site_zone_sum_voltage_change
    FROM windowed
)
-- Final projection
SELECT
    id,
    read_at,
    site_zone,
    site_commissioned_on,
    first_known_kwh,
    first_voltage,
    prev_kwh,
    kwh_share_of_quality_site_commissioned_on_year,
    distinct_site_zone_in_quality_site_commissioned_,
    first_known_kwh_vs_site_zone_avg,
    distinct_site_zone_in_quality_site_commissioned__2,
    CONCAT(site_zone, '#', CAST(id AS STRING)) AS site_zone_ref
FROM windowed_2
ORDER BY distinct_site_zone_in_quality_site_commissioned_
