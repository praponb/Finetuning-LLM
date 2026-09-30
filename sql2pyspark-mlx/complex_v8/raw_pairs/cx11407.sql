WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE quality = 'EST'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM base_readings
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND HOUR(m.read_at) BETWEEN 6 AND 8
        )
),
enriched_2 AS (
    -- Join sites
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 141.6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(kwh) OVER w_quality_group AS quality_group_max_kwh,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY quality_group)) AS distinct_quality_in_quality_group
    FROM enriched_2
    WINDOW w_quality_group AS (PARTITION BY quality_group)
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.site_commissioned_on,
        s.site_installed_kw,
        s.quality_group_max_kwh,
        s.distinct_quality_in_quality_group,
        si.tariff AS site_tariff,
        si.site_name AS site_site_name,
        si.commissioned_on AS site_commissioned_on_2
    FROM ranked s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    site_tariff,
    distinct_quality_in_quality_group,
    site_id,
    MIN(site_commissioned_on) AS first_site_commissioned_on,
    MAX(voltage) AS max_voltage
FROM with_details
GROUP BY GROUPING SETS ((site_tariff, distinct_quality_in_quality_group, site_id), (site_tariff), ())
ORDER BY site_tariff NULLS LAST, distinct_quality_in_quality_group NULLS LAST, site_id NULLS LAST
