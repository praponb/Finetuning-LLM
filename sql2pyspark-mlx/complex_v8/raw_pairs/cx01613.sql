WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-08'
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned_readings s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh BETWEEN 14.4 AND 117.2
              AND MONTH(m.read_at) IN (2, 3)
        )
      AND s.quality IN ('EST', 'OK')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        voltage,
        quality,
        kwh_adj,
        MAX(kwh_adj) OVER (ORDER BY read_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_adj_max_3_prior,
        voltage - LAG(voltage) OVER (ORDER BY read_at ASC, id) AS voltage_change,
        kwh_adj - LAG(kwh_adj) OVER (ORDER BY read_at ASC, id) AS kwh_adj_change
    FROM qualified
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE voltage < (SELECT MAX(voltage) FROM windowed)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM filtered
    WHERE quality <> 'EST'
)
SELECT *
FROM (
    SELECT read_at, quality, kwh_adj_max_3_prior
    FROM annotated
) src
PIVOT (
    SUM(kwh_adj_max_3_prior)
    FOR quality IN ('BAD', 'EST')
)
ORDER BY read_at
