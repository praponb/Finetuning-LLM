WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM meter_readings
    WHERE site_id <= 3
      AND MONTH(read_at) IN (1, 3)
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh > 14.4
              AND m.quality IN ('OK', 'EST')
        )
),
quality_stats AS (
    -- Avg kwh per quality
    SELECT quality, AVG(kwh) AS avg_kwh
    FROM filtered
    GROUP BY quality
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM filtered s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh > g.avg_kwh
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_adj,
        s.avg_kwh,
        s.kwh_minus_avg,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM compared s
    LEFT JOIN sites si ON si.id = s.site_id
),
unique_rows AS (
    -- Keep the latest row per site_zone
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh_adj,
        avg_kwh,
        kwh_minus_avg,
        site_commissioned_on,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone ORDER BY avg_kwh DESC, id DESC) AS row_num
        FROM with_parent_site
    ) ranked
    WHERE row_num = 1
)
SELECT id, metric, metric_value
FROM (
    SELECT id, kwh_adj, avg_kwh, kwh_minus_avg
    FROM unique_rows
) src
UNPIVOT (
    metric_value FOR metric IN (kwh_adj AS kwh_adj_v, avg_kwh AS m_avg_kwh, kwh_minus_avg AS m_kwh_minus_avg)
) unp
ORDER BY id, metric
