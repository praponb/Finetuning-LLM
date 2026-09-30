WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
    WHERE quality <> 'EST'
      AND site_id <= 7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        days_since_read_at,
        greatest_kwh_voltage,
        read_at_quarter,
        ROUND(days_since_read_at - AVG(days_since_read_at) OVER w_site_id, 2) AS days_since_read_at_vs_site_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY days_since_read_at DESC), 3) AS days_since_read_at_cume_dist,
        NTH_VALUE(days_since_read_at, 2) OVER (PARTITION BY site_id ORDER BY days_since_read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_read_at,
        MAX(voltage) OVER (PARTITION BY site_id) AS site_id_max_voltage,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id
    FROM base
    WINDOW w_site_id AS (PARTITION BY site_id)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE kwh > (SELECT MIN(kwh) FROM ranked)
),
site_id_stats AS (
    -- Max kwh per site_id
    SELECT site_id, MAX(kwh) AS max_kwh
    FROM filtered
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM filtered s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh = g.max_kwh
)
SELECT id, field, metric_value
FROM (
    SELECT id, max_kwh, days_since_read_at_cume_dist, kwh_minus_max, kwh
    FROM compared
) src
UNPIVOT (
    metric_value FOR field IN (max_kwh AS max_kwh_v, days_since_read_at_cume_dist AS m_days_since_read_at_cume_dist, kwh_minus_max AS kwh_minus_max_v, kwh AS m_kwh)
) unp
ORDER BY id, field
