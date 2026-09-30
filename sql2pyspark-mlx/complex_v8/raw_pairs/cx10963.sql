WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE voltage < 231.7
),
quality_stats AS (
    -- Max kwh per quality
    SELECT quality, MAX(kwh) AS max_kwh
    FROM scoped_readings
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM scoped_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh = g.max_kwh
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND TO_DATE(m.read_at) < DATE '2025-02-12'
              AND m.voltage < 227.6
        )
      AND s.max_kwh <= 78.0
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_quality_read_at_month_by_vol AS voltage_rank,
        RANK() OVER w_site_id_by_voltage_ties AS voltage_rank_2,
        ROUND(AVG(kwh_minus_max) OVER (PARTITION BY site_id), 2) AS site_id_avg_kwh_minus_max
    FROM with_activity
    WINDOW w_quality_read_at_month_by_vol AS (PARTITION BY quality, MONTH(read_at) ORDER BY voltage NULLS LAST, id), w_site_id_by_voltage_ties AS (PARTITION BY site_id ORDER BY voltage DESC)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    kwh_minus_max,
    voltage_rank,
    voltage_rank_2,
    site_id_avg_kwh_minus_max,
    DATE_TRUNC('DAY', read_at) AS read_at_day,
    ROUND(kwh_minus_max / NULLIF(kwh, 0), 3) AS kwh_minus_max_per_kwh
FROM sequenced
WHERE kwh_minus_max < 84.0
ORDER BY read_at DESC, kwh, id
LIMIT 10
