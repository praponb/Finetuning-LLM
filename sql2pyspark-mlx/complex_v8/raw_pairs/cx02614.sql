WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        kwh + voltage AS kwh_plus_voltage
    FROM meter_readings
    WHERE kwh < 84.9
      AND MONTH(read_at) IN (1, 3)
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM prepared_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 87.1
              AND m.voltage BETWEEN 219.5 AND 240.1
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY voltage ASC), 3) AS voltage_percent_rank,
        ROUND(CUME_DIST() OVER w_quality_by_voltage, 3) AS voltage_cume_dist,
        LAST_VALUE(voltage) OVER (PARTITION BY quality ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        FIRST_VALUE(read_at) OVER (PARTITION BY quality ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, TO_DATE(read_at))) AS distinct_quality_in_site_id_read_at_to_date,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY voltage ASC, id) AS voltage_dense_rank
    FROM screened
    WINDOW w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage ASC, id)
)
-- Subtotals with CUBE
SELECT
    site_id,
    voltage_dense_rank,
    MIN(read_at) AS first_read_at_2,
    COUNT(*) AS n_rows
FROM with_windows
GROUP BY CUBE(site_id, voltage_dense_rank)
ORDER BY site_id NULLS LAST, voltage_dense_rank NULLS LAST
