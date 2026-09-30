WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 233.2
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM scoped
    WHERE quality = 'OK'
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING(site_id) AS is_total_site_id,
    MAX(kwh_bucket) AS max_kwh_bucket,
    SUM(kwh_bucket) AS total_kwh_bucket
FROM annotated
GROUP BY GROUPING SETS ((site_id, quality), (site_id), (quality))
ORDER BY site_id NULLS LAST, quality NULLS LAST
