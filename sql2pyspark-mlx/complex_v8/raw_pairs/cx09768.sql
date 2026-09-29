WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_site_id_quality_by_read_at AS read_at_rank,
        LAG(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id) AS prev2_read_at,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY read_at DESC), 3) AS read_at_cume_dist
    FROM base
    WINDOW w_site_id_quality_by_read_at AS (PARTITION BY site_id, quality ORDER BY read_at DESC, id)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, SUM(kwh) AS total_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.total_reading_kwh
    FROM metrics s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 233.8
        )
      AND s.total_reading_kwh BETWEEN 216.0 AND 432.0
)
-- Subtotals with GROUPING SETS
SELECT
    quality_len,
    COALESCE(quality, 'ALL') AS quality,
    COUNT(*) AS cnt,
    COUNT(DISTINCT quality) AS distinct_qualitys
FROM screened
GROUP BY GROUPING SETS ((quality_len, quality), (quality_len), (quality))
ORDER BY quality_len NULLS LAST, quality NULLS LAST
