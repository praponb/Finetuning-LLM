WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ABS(voltage - 233.1) AS voltage_distance
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 234.5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_quality, 0), 4) AS voltage_share_of_site_id_quality,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY read_at NULLS LAST) AS read_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_read_at,
        SUM(voltage_distance) OVER w_site_id_quality AS site_id_quality_sum_voltage_distance
    FROM scoped
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE read_at_dense_rank > (SELECT MIN(read_at_dense_rank) FROM metrics)
)
-- Subtotals with CUBE
SELECT
    read_at_dense_rank,
    site_id,
    ROUND(SUM(voltage_share_of_site_id_quality) / NULLIF(SUM(rows_in_site_id_quality), 0), 3) AS voltage_share_of_site_id_quality_to_rows_in_site
FROM screened
GROUP BY CUBE(read_at_dense_rank, site_id)
ORDER BY read_at_dense_rank NULLS LAST, site_id NULLS LAST
