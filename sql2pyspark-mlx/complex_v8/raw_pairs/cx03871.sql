WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CASE WHEN voltage >= 232.0 THEN 'large' WHEN voltage >= 218.4 THEN 'medium' ELSE 'small' END AS voltage_tier,
        ROUND(voltage * 0.25, 2) AS voltage_adj
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        voltage_tier,
        MIN(read_at) AS first_read_at,
        SUM(CASE WHEN quality IS NOT NULL THEN 1 ELSE 0 END) AS n_quality_is_not_null,
        ROUND(100 * COUNT(CASE WHEN quality = 'OK' THEN 1 END) / COUNT(*), 1) AS pct_quality_ok
    FROM filtered
    GROUP BY voltage_tier
),
labeled AS (
    -- Label and score rows
    SELECT
        voltage_tier,
        first_read_at,
        n_quality_is_not_null,
        pct_quality_ok,
        ROUND(n_quality_is_not_null / NULLIF(pct_quality_ok, 0), 3) AS n_quality_is_not_null_per_pct_quality_ok
    FROM per_group
    WHERE pct_quality_ok >= 30.0
),
labeled_2 AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(voltage_tier), 1, 2) AS voltage_tier_prefix
    FROM labeled
    WHERE pct_quality_ok <= 85.0
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_quality_is_not_null, voltage_tier, n_quality_is_not_null_per_pct_quality_ok
    FROM labeled_2
) src
PIVOT (
    COUNT(n_quality_is_not_null_per_pct_quality_ok)
    FOR voltage_tier IN ('small', 'medium')
)
