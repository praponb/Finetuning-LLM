WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
quality_stats AS (
    -- Avg kwh_adj per quality
    SELECT quality, AVG(kwh_adj) AS avg_kwh_adj
    FROM cleaned_readings
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_kwh_adj,
        ROUND(s.kwh_adj - g.avg_kwh_adj, 2) AS kwh_adj_minus_avg
    FROM cleaned_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh_adj > g.avg_kwh_adj
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality,
        LAG(read_at, 2) OVER w_site_id_by_avg_kwh_adj AS prev2_read_at,
        LEAD(kwh, 2) OVER (PARTITION BY site_id ORDER BY avg_kwh_adj ASC, id) AS next2_kwh
    FROM outliers
    WINDOW w_site_id_by_avg_kwh_adj AS (PARTITION BY site_id ORDER BY avg_kwh_adj ASC, id)
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 79.7
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, kwh, kwh_adj_minus_avg, least_voltage_kwh, avg_kwh_adj
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (kwh, kwh_adj_minus_avg, least_voltage_kwh, avg_kwh_adj)
) unp
