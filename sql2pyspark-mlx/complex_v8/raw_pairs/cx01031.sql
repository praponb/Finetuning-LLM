WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        RANK() OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY voltage) AS voltage_rank
    FROM recent_readings
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on < DATE '2025-11-28'
        )
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality_group,
    quality_slug,
    quality_clean,
    kwh_avg_1_prior,
    voltage_rank
FROM filtered
