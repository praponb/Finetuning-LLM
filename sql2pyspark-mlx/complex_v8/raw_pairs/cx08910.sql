WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ABS(voltage - 229.1) AS voltage_distance,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE (site_id >= 2 OR quality <> 'EST')
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned_readings s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh >= 52.4
        )
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 7 AND 13
              AND m.quality = 'BAD'
        )
      AND s.quality IS NOT NULL
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.quality_prefix,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.site_name
    FROM screened s
    LEFT JOIN sites si ON si.id = s.site_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        site_id,
        site_zone,
        COUNT(*) FILTER (WHERE site_tariff NOT IN ('FLAT', 'EV')) AS n_site_tariff_not_in_flat_ev,
        ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count,
        SUM(CASE WHEN quality = 'BAD' THEN 1 ELSE 0 END) AS n_quality_bad
    FROM combined_details_site
    GROUP BY site_id, site_zone
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(site_zone), 1, 4) AS site_zone_prefix,
        INITCAP(TRIM(site_zone)) AS site_zone_clean
    FROM rollup_base
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM scored s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    site_id,
    n_site_tariff_not_in_flat_ev,
    site_tariff_list,
    row_count,
    site_zone_prefix,
    site_zone_clean
FROM with_counts
ORDER BY site_zone_clean, site_zone_prefix DESC
