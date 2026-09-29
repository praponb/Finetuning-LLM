WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'BAD')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(read_at_year), site_id) AS rows_in_read_at_year_month_site_id,
        kwh - LAG(kwh) OVER (PARTITION BY quality, TO_DATE(read_at) ORDER BY voltage DESC, id) AS kwh_change,
        voltage - LAG(voltage) OVER (ORDER BY read_at DESC, id) AS voltage_change,
        LAST_VALUE(read_at) OVER (PARTITION BY quality, TO_DATE(read_at) ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at
    FROM cleaned
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(kwh, voltage_change) AS least_kwh_voltage_change,
        DATE_FORMAT(read_at_year, 'MMM yyyy') AS read_at_year_label
    FROM with_windows
    WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
)
SELECT
    site_id,
    quality,
    GROUPING_ID() AS gid,
    PERCENTILE_APPROX(rows_in_read_at_year_month_site_id, 0.5) AS median_rows_in_read_at_year_month_site_id,
    ROUND(SUM(voltage) / NULLIF(SUM(kwh_change), 0), 3) AS voltage_to_kwh_change_ratio,
    MIN(read_at) AS first_read_at
FROM enriched
GROUP BY CUBE(site_id, quality)
ORDER BY site_id NULLS LAST, quality NULLS LAST
