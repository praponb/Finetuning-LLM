WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE quality = 'OK'
      AND kwh <= 109.0
),
deduped AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_clean,
        quality_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh DESC, id DESC) AS rn
        FROM filtered_readings
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Label and score rows
    SELECT *, GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM deduped
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality <> 'BAD'
        )
      AND LOWER(s.quality_clean) LIKE '%ad%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_group,
        greatest_kwh_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality_group ORDER BY read_at DESC), 3) AS read_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality_group ORDER BY read_at DESC), 3) AS read_at_cume_dist
    FROM without_matches
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_group,
        s.greatest_kwh_voltage,
        s.read_at_percent_rank,
        s.read_at_cume_dist,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_group,
    greatest_kwh_voltage,
    read_at_percent_rank,
    read_at_cume_dist,
    site_zone,
    site_commissioned_on,
    site_tariff
FROM with_details_site
ORDER BY read_at DESC
