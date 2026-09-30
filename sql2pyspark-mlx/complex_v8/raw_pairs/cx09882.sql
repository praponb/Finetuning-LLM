WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE kwh < 106.5
),
unique_rows AS (
    -- Keep the earliest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY read_at, id) AS rn
        FROM cleaned_readings
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-02-06 15:00:00'
              AND m.voltage BETWEEN 213.5 AND 233.6
        )
      AND s.kwh >= 33.9
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage ASC) AS voltage_dense_rank,
        ROUND(AVG(kwh) OVER w_site_id_quality, 2) AS site_id_quality_avg_kwh
    FROM with_activity
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
-- Subtotals with CUBE
SELECT
    COALESCE(quality, 'ALL') AS quality,
    voltage_dense_rank,
    site_id,
    MAX(read_at) AS last_read_at,
    MAX(site_id_quality_avg_kwh) AS max_site_id_quality_avg_kwh,
    ROUND(100 * COUNT(CASE WHEN quality NOT IN ('EST', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_est_bad
FROM metrics
GROUP BY CUBE(quality, voltage_dense_rank, site_id)
