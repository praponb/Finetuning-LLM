WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('DAY', read_at) AS read_at_day,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE voltage < 232.4
      AND HOUR(read_at) BETWEEN 7 AND 9
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_day,
        s.quality_or_unknown,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM valid s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS voltage_share_of_read_at_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY read_at_day, id), 3) AS read_at_day_percent_rank
    FROM combined_details
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY quality, site_zone ORDER BY read_at_day) AS read_at_day_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_zone ORDER BY read_at_day, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        MIN(kwh) OVER w_quality_or_unknown_site_tari AS quality_or_unknown_site_tariff_min_kwh,
        COUNT(*) OVER (PARTITION BY quality_or_unknown, site_tariff) AS rows_in_quality_or_unknown_site_tariff,
        LEAD(voltage, 2) OVER (ORDER BY voltage_share_of_read_at_year ASC, id) AS next2_voltage,
        LEAD(kwh) OVER (ORDER BY voltage_share_of_read_at_year ASC, id) AS next_kwh
    FROM ranked
    WINDOW w_quality_or_unknown_site_tari AS (PARTITION BY quality_or_unknown, site_tariff)
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_day,
        s.quality_or_unknown,
        s.site_zone,
        s.voltage_share_of_read_at_year,
        s.read_at_day_percent_rank,
        s.read_at_day_rank,
        s.quality_or_unknown_site_tariff_min_kwh,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff_2,
        si.commissioned_on AS site_commissioned_on_2
    FROM ranked_2 s
    JOIN sites si ON si.id = s.site_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        voltage,
        quality,
        quality_or_unknown,
        site_zone,
        read_at_day_percent_rank,
        read_at_day_rank,
        site_installed_kw,
        site_tariff_2,
        site_commissioned_on_2,
        DAYOFWEEK(site_commissioned_on_2) AS site_commissioned_on_2_dayofweek
    FROM with_parent_site
)
-- Final projection
SELECT DISTINCT site_commissioned_on_2_dayofweek
FROM labeled
WHERE read_at_day_percent_rank >= 0.2
ORDER BY site_commissioned_on_2_dayofweek
