WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE (kwh >= 34.6 OR voltage <= 231.7)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(kwh_bucket) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_kwh_bucket
    FROM cleaned_readings
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(quality_group, '#', CAST(id AS STRING)) AS quality_group_ref,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        TO_DATE(read_at) AS read_at_date
    FROM windowed
    WHERE quality_group NOT IN ('group_b', 'other')
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
enriched AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM qualified s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    quality_group,
    kwh_bucket,
    quality_site_id_sum_kwh_bucket,
    quality_group_ref,
    days_since_read_at,
    read_at_date,
    site_installed_kw,
    site_tariff,
    site_zone,
    DATE_TRUNC('MONTH', read_at_date) AS read_at_date_month,
    FLOOR(kwh / 25) * 25 AS kwh_bucket_2
FROM enriched
WHERE site_zone <> 'Z4'
ORDER BY id DESC, kwh_bucket_2 DESC NULLS FIRST
