WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.zone AS site_zone,
        si.site_name
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        MAX(voltage) OVER (PARTITION BY site_id, site_zone ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_max_1_prior,
        ROUND(AVG(voltage) OVER (PARTITION BY quality ORDER BY voltage DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at) ORDER BY voltage, id), 3) AS voltage_cume_dist,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, site_zone)) AS distinct_site_id_in_quality_site_zone,
        NTILE(4) OVER (PARTITION BY quality ORDER BY voltage DESC, id) AS voltage_quartile,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_zone), 0), 4) AS voltage_share_of_quality_site_zone,
        COUNT(*) OVER w_quality_site_zone AS rows_in_quality_site_zone,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_2,
        ROUND(CUME_DIST() OVER (ORDER BY read_at ASC NULLS LAST), 3) AS read_at_cume_dist,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4
    FROM with_parent_site
    WINDOW w_quality_site_zone AS (PARTITION BY quality, site_zone)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        MONTH(read_at) AS read_at_month,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM windowed
    WHERE rows_in_quality_site_zone BETWEEN 3 AND 14
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY read_at_month, voltage_quartile) AS rows_in_read_at_month_voltage_quartile,
        MAX(voltage_avg_4) OVER (PARTITION BY distinct_site_id_in_quality_site_zone, read_at_month ORDER BY read_at_year DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_avg_4_max_1_prior,
        ROUND(voltage_avg_7 - AVG(voltage_avg_7) OVER (PARTITION BY distinct_site_id_in_quality_site_zone, read_at_month), 2) AS voltage_avg_7_vs_distinct_site_id_in_quality_sit,
        ROUND(read_at_cume_dist - AVG(read_at_cume_dist) OVER w_distinct_site_id_in_quality_2, 2) AS read_at_cume_dist_vs_distinct_site_id_in_quality
    FROM tagged
    WINDOW w_distinct_site_id_in_quality_2 AS (PARTITION BY distinct_site_id_in_quality_site_zone, read_at_month)
)
SELECT
    id,
    voltage,
    quality,
    distinct_quality_in_site_id,
    voltage_max_1_prior,
    voltage_cume_dist,
    distinct_site_id_in_quality_site_zone,
    voltage_quartile,
    voltage_share_of_quality_site_zone,
    rows_in_quality_site_zone,
    voltage_avg_4,
    read_at_year,
    quality_slug,
    voltage_avg_4_max_1_prior,
    voltage_avg_7_vs_distinct_site_id_in_quality_sit,
    read_at_cume_dist_vs_distinct_site_id_in_quality,
    CONCAT_WS('-', quality, quality_slug) AS quality_quality_slug_label
FROM sequenced
ORDER BY read_at_year, rows_in_quality_site_zone DESC
