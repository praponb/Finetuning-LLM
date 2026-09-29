WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN kwh >= 101.2 THEN 'large' WHEN kwh >= 35.9 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM filtered_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
quality_stats AS (
    -- Max kwh per quality
    SELECT quality, MAX(kwh) AS max_kwh
    FROM sessionized
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM sessionized s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh = g.max_kwh
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        kwh,
        quality,
        kwh_tier,
        prev_read_at,
        session_id,
        max_kwh,
        kwh_minus_max,
        LENGTH(TRIM(kwh_tier)) AS kwh_tier_len,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CONCAT_WS('-', kwh_tier, quality) AS kwh_tier_quality_label
    FROM outliers
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, kwh_tier_len, session_id
    FROM tagged
) src
UNPIVOT (
    reading FOR metric IN (kwh_tier_len, session_id)
) unp
