WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-18'
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_len,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM sessionized s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage > 216.1
),
reading_quality_stats AS (
    -- Avg kwh per reading_quality
    SELECT reading_quality, AVG(kwh) AS avg_kwh
    FROM with_children_readings
    GROUP BY reading_quality
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM with_children_readings s
    JOIN reading_quality_stats g ON g.reading_quality = s.reading_quality
    WHERE s.kwh > g.avg_kwh
)
SELECT
    reading_quality,
    site_id,
    quality_len,
    COUNT(*) AS n_rows
FROM compared
GROUP BY ROLLUP(reading_quality, site_id, quality_len)
ORDER BY reading_quality NULLS LAST, site_id NULLS LAST, quality_len NULLS LAST
