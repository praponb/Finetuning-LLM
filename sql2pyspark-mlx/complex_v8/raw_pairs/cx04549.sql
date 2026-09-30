WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE (quality <> 'EST' OR kwh > 46.6)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(voltage, 3) OVER w_all_by_voltage AS prev3_voltage,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        MAX(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_kwh
    FROM filtered_readings
    WINDOW w_all_by_voltage AS (ORDER BY voltage DESC, id), w_site_id_quality AS (PARTITION BY site_id, quality)
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    quality,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count
FROM sequenced
GROUP BY GROUPING SETS ((site_id, quality), (site_id), (quality), ())
