WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
),
summary AS (
    -- Summarise per group
    SELECT
        quality,
        site_id,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MIN(kwh) AS min_kwh,
        ROUND(100 * COUNT(CASE WHEN quality NOT IN ('BAD', 'OK') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_bad_ok,
        COUNT(*) AS n_rows
    FROM scoped
    GROUP BY quality, site_id
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IN ('OK', 'EST')
              AND m.kwh BETWEEN 6.8 AND 117.1
        )
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.quality,
        s.site_id,
        s.min_kwh,
        s.pct_quality_not_in_bad_ok,
        s.n_rows,
        si.tariff AS site_tariff
    FROM without_matches s
    JOIN sites si ON si.id = s.site_id
),
with_details AS (
    -- Attach site attributes
    SELECT s.quality, s.site_id, si.zone AS site_zone
    FROM combined_details_site s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT quality, site_id
FROM with_details
ORDER BY site_id
