WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR kwh <= 113.1)
),
quality_stats AS (
    -- Max kwh per quality
    SELECT quality, MAX(kwh) AS max_kwh
    FROM cleaned_readings
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM cleaned_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh = g.max_kwh
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(kwh_minus_max) OVER w_site_id, 2) AS site_id_avg_kwh_minus_max,
        RANK() OVER (PARTITION BY quality ORDER BY max_kwh, id) AS max_kwh_rank
    FROM above_average
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    kwh_minus_max,
    max_kwh_rank,
    kwh - max_kwh AS kwh_minus_max_kwh
FROM windowed
ORDER BY quality DESC, kwh DESC
