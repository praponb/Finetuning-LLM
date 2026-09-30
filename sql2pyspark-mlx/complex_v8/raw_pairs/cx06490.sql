WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'EST')
      AND kwh >= 24.5
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_readings
    WHERE kwh < (SELECT MAX(kwh) FROM filtered_readings)
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    quality,
    GROUPING_ID() AS gid,
    SUM(voltage) FILTER (WHERE quality <> 'EST') AS voltage_quality_est,
    SUM(CASE WHEN quality = 'OK' THEN 1 ELSE 0 END) AS n_quality_ok,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM without_matches
GROUP BY GROUPING SETS ((site_id, quality), (site_id), ())
ORDER BY site_id NULLS LAST, quality NULLS LAST
