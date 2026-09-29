WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE quality = 'EST'
      AND site_id >= 2
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM valid_readings
    WHERE kwh > (SELECT MIN(kwh) FROM valid_readings)
)
-- Subtotals with CUBE
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad
FROM without_matches
GROUP BY CUBE(site_id, quality)
ORDER BY site_id NULLS LAST, quality NULLS LAST
