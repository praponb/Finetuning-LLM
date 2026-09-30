WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        ABS(voltage - 222.3) AS voltage_distance
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
      AND site_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_year,
        voltage_distance,
        NTILE(4) OVER (PARTITION BY MONTH(read_at_year) ORDER BY read_at, id) AS read_at_quartile,
        LEAD(voltage_distance) OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id) AS next_voltage_distance
    FROM base_readings
),
per_group AS (
    -- Summarise per group
    SELECT
        site_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
        SUM(read_at_quartile) FILTER (WHERE quality IS NOT NULL) AS read_at_quartile_quality_is_not_null
    FROM with_trends
    GROUP BY site_id
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM per_group s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.read_at_quartile_quality_is_not_null >= 5
),
joined AS (
    -- Attach site attributes
    SELECT
        s.site_id,
        s.n_rows,
        s.distinct_site_ids,
        s.n_quality_is_not_null,
        s.read_at_quartile_quality_is_not_null,
        si.site_name,
        si.zone AS site_zone
    FROM screened s
    JOIN sites si ON si.id = s.site_id AND si.zone = 'Z2'
    WHERE si.zone = 'Z3'
)
SELECT
    site_id,
    n_rows,
    n_quality_is_not_null,
    read_at_quartile_quality_is_not_null,
    site_name
FROM joined
ORDER BY site_name DESC, site_id
LIMIT 5
