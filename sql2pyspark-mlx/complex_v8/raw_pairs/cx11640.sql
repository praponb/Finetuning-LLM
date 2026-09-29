WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        TO_DATE(read_at) AS read_at_date,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug_2
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND read_at < TIMESTAMP '2025-02-24 10:00:00'
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage > 214.3
        )
),
site_zone_stats AS (
    -- Max kwh per site_zone
    SELECT site_zone, MAX(kwh) AS max_kwh
    FROM filtered
    GROUP BY site_zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM filtered s
    JOIN site_zone_stats g ON g.site_zone = s.site_zone
    WHERE s.kwh = g.max_kwh
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        MAX(max_kwh) AS max_max_kwh,
        DATEDIFF(MAX(read_at_date), MIN(read_at_date)) AS read_at_date_span_days,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(site_zone)) AS site_zone_list
    FROM outliers
    GROUP BY site_id
    HAVING SUM(kwh_minus_max) > -48.0
)
SELECT
    site_id,
    site_zone_list,
    ROUND(max_max_kwh / NULLIF(n_rows, 0), 3) AS max_max_kwh_per_n_rows
FROM rollup_base
WHERE max_max_kwh <= 90.0
ORDER BY max_max_kwh_per_n_rows, site_id
