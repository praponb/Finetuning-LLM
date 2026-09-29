WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        kwh - voltage AS kwh_minus_voltage,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 236.7
),
grouped AS (
    -- Group-level totals
    SELECT
        site_id,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        quality,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MIN(voltage) AS min_voltage
    FROM recent
    GROUP BY 1, 2, 3
    HAVING AVG(kwh_minus_voltage) > 146.0
)
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING(site_id) AS is_total_site_id,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list_2,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM grouped
GROUP BY GROUPING SETS ((site_id, quality), (site_id))
