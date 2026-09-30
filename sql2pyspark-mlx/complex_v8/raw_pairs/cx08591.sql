WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND voltage BETWEEN 213.5 AND 234.7
),
rollup_base AS (
    -- Summarise per group
    SELECT
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        site_id,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN quality = 'BAD' THEN 1 END) / COUNT(*), 1) AS pct_quality_bad,
        MIN(voltage) AS min_voltage,
        MIN(read_at) AS first_read_at,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage), 0), 3) AS kwh_to_voltage_ratio
    FROM filtered
    GROUP BY quality, DATE_FORMAT(read_at, 'yyyy-MM'), site_id
    HAVING COUNT(*) >= 3
),
with_details_site AS (
    -- Join sites
    SELECT
        s.quality,
        s.read_at_ym,
        s.site_id,
        s.n_rows,
        s.first_read_at,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM rollup_base s
    JOIN sites si ON si.id = s.site_id AND UPPER(TRIM(si.site_name)) LIKE 'D%'
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(CASE WHEN m.quality <> 'EST' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM with_details_site s
)
SELECT
    quality,
    read_at_ym,
    site_id,
    n_rows,
    first_read_at,
    site_zone,
    site_tariff,
    reading_count,
    CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
    DATEDIFF(DATE '2025-06-30', first_read_at) AS days_since_first_read_at
FROM with_counts
ORDER BY n_rows DESC, reading_count, quality, read_at_ym, site_id
LIMIT 5
