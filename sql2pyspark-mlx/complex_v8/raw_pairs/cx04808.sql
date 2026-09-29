WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group_2
    FROM meter_readings
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(kwh - 48.2) AS kwh_distance,
        CONCAT(quality_group, '#', CAST(id AS STRING)) AS quality_group_ref
    FROM filtered
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh
    FROM scored s
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_group,
        s.quality_group_2,
        s.kwh_distance,
        s.avg_reading_kwh,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM with_counts s
    JOIN sites si ON si.id = s.site_id AND LENGTH(TRIM(si.site_name)) >= 7
    WHERE si.commissioned_on < DATE '2025-05-07'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality_group), ' ', '_')) AS quality_group_slug,
        SUBSTRING(TRIM(quality_group), 1, 2) AS quality_group_prefix
    FROM with_parent
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality_group,
        quality_group_2,
        kwh_distance,
        avg_reading_kwh,
        site_commissioned_on,
        quality_group_prefix,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality_group_2), 2) AS voltage_vs_quality_group_2_avg,
        LAG(read_at) OVER (PARTITION BY quality_group_2, quality_group ORDER BY kwh_distance ASC, id) AS prev_read_at
    FROM labeled
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(quality_group) OVER w_prev_read_at_month_quality_g) AS distinct_quality_group_in_prev_read_at_month_qua,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_read_at), quality_group_2) AS rows_in_prev_read_at_month_quality_group_2,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_read_at), quality_group_2), 2) AS voltage_vs_prev_read_at_month_quality_group_2_av
    FROM ranked
    WINDOW w_prev_read_at_month_quality_g AS (PARTITION BY DATE_TRUNC('MONTH', prev_read_at), quality_group_2)
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    quality_group_2,
    PERCENTILE_APPROX(avg_reading_kwh, 0.5) AS median_avg_reading_kwh
FROM ranked_2
GROUP BY GROUPING SETS ((site_id, quality_group_2), (site_id), (quality_group_2), ())
ORDER BY site_id NULLS LAST, quality_group_2 NULLS LAST
