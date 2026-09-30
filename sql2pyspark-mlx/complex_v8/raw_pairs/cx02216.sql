WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        ROUND(kwh * 0.25, 2) AS kwh_adj,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 07:00:00'
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM valid_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on < DATE '2024-11-21'
              AND UPPER(TRIM(si.site_name)) LIKE 'R%'
        )
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE voltage_bucket > (SELECT AVG(voltage_bucket) FROM without_matches)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        QUARTER(read_at) AS read_at_quarter,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        ROUND(voltage_bucket / NULLIF(kwh, 0), 3) AS voltage_bucket_per_kwh
    FROM screened
)
SELECT
    quality_group,
    read_at_quarter,
    quality,
    COUNT(*) FILTER (WHERE quality IN ('BAD', 'EST')) AS n_quality_in_bad_est
FROM tagged
GROUP BY GROUPING SETS ((quality_group, read_at_quarter, quality), (quality_group), (read_at_quarter))
