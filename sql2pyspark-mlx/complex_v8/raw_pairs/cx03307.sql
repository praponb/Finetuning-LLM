WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ABS(kwh - 80.8) AS kwh_distance
    FROM meter_readings
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(kwh_distance) OVER (PARTITION BY site_id, quality) AS site_id_quality_sum_kwh_distance,
        LAG(kwh_distance) OVER (PARTITION BY quality, site_id ORDER BY voltage ASC NULLS LAST, id) AS prev_kwh_distance,
        kwh_distance - LAG(kwh_distance) OVER (PARTITION BY quality, site_id ORDER BY voltage ASC NULLS LAST, id) AS kwh_distance_change
    FROM scoped
),
grouped AS (
    -- Summarise per group
    SELECT
        quality,
        TO_DATE(read_at) AS read_at_day,
        site_id,
        ROUND(SUM(prev_kwh_distance), 2) AS total_prev_kwh_distance,
        MAX(kwh_distance_change) AS max_kwh_distance_change,
        MIN(voltage) AS min_voltage,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    WHERE kwh_distance_change >= -36.0
    GROUP BY quality, TO_DATE(read_at), site_id
)
SELECT
    site_id,
    quality,
    distinct_ids,
    COUNT(*) AS n_rows,
    MAX(read_at_day) AS last_read_at_day,
    DATEDIFF(MAX(read_at_day), MIN(read_at_day)) AS read_at_day_span_days
FROM grouped
GROUP BY GROUPING SETS ((site_id, quality, distinct_ids), (site_id), (quality), ())
ORDER BY site_id NULLS LAST, quality NULLS LAST, distinct_ids NULLS LAST
