WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug_2,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE voltage < 235.0
),
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage
    FROM recent
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM recent s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh BETWEEN 37.0 AND 113.0
              AND m.quality NOT IN ('BAD', 'OK')
        )
      AND s.avg_voltage > 213.5
),
site_id_stats_2 AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage_2
    FROM screened
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage_2,
        ROUND(s.voltage - g.avg_voltage_2, 2) AS voltage_minus_avg_2
    FROM screened s
    JOIN site_id_stats_2 g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage_2
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        MIN(read_at) AS first_read_at,
        DATEDIFF(MAX(read_at_date), MIN(read_at_date)) AS read_at_date_span_days,
        MIN(read_at_date) AS first_read_at_date
    FROM vs_group
    GROUP BY DATE_TRUNC('MONTH', read_at)
    HAVING AVG(kwh) > 50.4
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE read_at_date_span_days > (SELECT AVG(read_at_date_span_days) FROM rollup_base)
)
SELECT
    read_at_month,
    first_read_at,
    first_read_at_date,
    CASE WHEN read_at_date_span_days >= 385 THEN 'large' WHEN read_at_date_span_days >= 280 THEN 'medium' ELSE 'small' END AS read_at_date_span_days_tier
FROM screened_2
ORDER BY read_at_month DESC, first_read_at_date NULLS LAST
