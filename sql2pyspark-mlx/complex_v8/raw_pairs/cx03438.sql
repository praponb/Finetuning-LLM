WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 15 MINUTES AS read_at_plus_15m
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        burst_id,
        COUNT(*) OVER (PARTITION BY YEAR(read_at_plus_15m), burst_id) AS rows_in_read_at_plus_15m_year_burst_id,
        SIZE(COLLECT_SET(quality) OVER w_read_at_plus_15m_year_burst) AS distinct_quality_in_read_at_plus_15m_year_burst_,
        ROUND(burst_id / NULLIF(SUM(burst_id) OVER (PARTITION BY YEAR(read_at_plus_15m), burst_id), 0), 4) AS burst_id_share_of_read_at_plus_15m_year_burst_id
    FROM sessionized
    WINDOW w_read_at_plus_15m_year_burst AS (PARTITION BY YEAR(read_at_plus_15m), burst_id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.burst_id,
        s.rows_in_read_at_plus_15m_year_burst_id,
        s.distinct_quality_in_read_at_plus_15m_year_burst_,
        si.tariff AS site_tariff
    FROM windowed s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    burst_id,
    rows_in_read_at_plus_15m_year_burst_id,
    distinct_quality_in_read_at_plus_15m_year_burst_,
    site_tariff,
    DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
    LENGTH(TRIM(site_tariff)) AS site_tariff_len
FROM with_parent
ORDER BY rows_in_read_at_plus_15m_year_burst_id NULLS LAST
