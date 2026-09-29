WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        LENGTH(TRIM(quality)) AS quality_len,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE quality IN ('EST', 'BAD')
      AND kwh <= 81.1
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_len,
        s.read_at_date,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
filtered_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 220.2 AND 233.2
              AND m.quality IN ('OK', 'BAD')
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(quality_len) OVER (PARTITION BY site_id, quality ORDER BY read_at_date ASC NULLS LAST, id) AS prev_quality_len,
        ROUND(quality_len - AVG(quality_len) OVER (PARTITION BY quality_len), 2) AS quality_len_vs_quality_len_avg,
        SUM(site_installed_kw) OVER (PARTITION BY site_id ORDER BY quality_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw,
        NTH_VALUE(read_at_date, 2) OVER (PARTITION BY quality_len ORDER BY read_at_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_date
    FROM filtered_2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_len,
        s.site_name,
        s.quality_len_vs_quality_len_avg,
        s.running_site_installed_kw,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    site_name,
    quality_len_vs_quality_len_avg,
    running_site_installed_kw,
    site_tariff,
    site_zone,
    site_commissioned_on,
    FLOOR(voltage / 5) * 5 AS voltage_bucket,
    CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
FROM enriched
WHERE site_id IS NOT NULL
ORDER BY site_tariff DESC, quality_len_vs_quality_len_avg DESC NULLS FIRST
