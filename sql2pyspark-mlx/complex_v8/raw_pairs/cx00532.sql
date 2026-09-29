WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        CASE WHEN kwh >= 87.1 THEN 'A' WHEN kwh >= 50.4 THEN 'B' ELSE 'C' END AS kwh_tier,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        quality,
        quality_len,
        kwh_tier,
        read_at_year,
        voltage - kwh AS voltage_minus_kwh
    FROM prepared_readings
),
kwh_tier_stats AS (
    -- Avg kwh per kwh_tier
    SELECT kwh_tier, AVG(kwh) AS avg_kwh
    FROM annotated
    GROUP BY kwh_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM annotated s
    JOIN kwh_tier_stats g ON g.kwh_tier = s.kwh_tier
    WHERE s.kwh > g.avg_kwh
)
SELECT id, metric, metric_value
FROM (
    SELECT id, quality_len, read_at_year
    FROM vs_group
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (quality_len, read_at_year)
) unp
