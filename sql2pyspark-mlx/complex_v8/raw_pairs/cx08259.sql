WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE voltage >= 222.3
),
summary AS (
    -- Group-level totals
    SELECT
        read_at_year,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(voltage) / NULLIF(SUM(read_at_year), 0), 3) AS voltage_to_read_at_year_ratio,
        ROUND(100 * COUNT(CASE WHEN quality = 'BAD' THEN 1 END) / COUNT(*), 1) AS pct_quality_bad,
        SUM(voltage) FILTER (WHERE quality IN ('BAD', 'EST')) AS voltage_quality_in_bad_est,
        SUM(CASE WHEN quality = 'BAD' THEN voltage ELSE 0 END) AS voltage_quality_bad
    FROM cleaned
    WHERE quality_prefix IS NOT NULL
    GROUP BY 1
    HAVING MAX(voltage) > 220.2
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(voltage_quality_bad - 367.5) AS voltage_quality_bad_distance,
        ROUND(voltage_quality_bad * 1.07, 2) AS voltage_quality_bad_adj
    FROM summary
    WHERE pct_quality_bad <= 55.0
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE pct_quality_bad > (SELECT AVG(pct_quality_bad) FROM labeled)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(voltage_quality_in_bad_est) OVER w_read_at_year_by_voltage_qual AS prev_voltage_quality_in_bad_est,
        FIRST_VALUE(voltage_quality_bad_adj) IGNORE NULLS OVER (PARTITION BY read_at_year ORDER BY voltage_quality_bad_distance, read_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_quality_bad_adj
    FROM with_activity
    WINDOW w_read_at_year_by_voltage_qual AS (PARTITION BY read_at_year ORDER BY voltage_quality_bad_distance, read_at_year)
)
SELECT
    read_at_year,
    quality_list,
    pct_quality_bad,
    voltage_quality_bad_distance,
    prev_voltage_quality_in_bad_est,
    first_known_voltage_quality_bad_adj
FROM ranked
ORDER BY first_known_voltage_quality_bad_adj NULLS LAST, read_at_year
LIMIT 10
