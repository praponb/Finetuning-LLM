WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        voltage - kwh AS voltage_minus_kwh,
        CASE WHEN kwh >= 84.9 THEN 'large' WHEN kwh >= 33.9 THEN 'medium' ELSE 'small' END AS kwh_tier,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
kwh_tier_stats AS (
    -- Max kwh per kwh_tier
    SELECT kwh_tier, MAX(kwh) AS max_kwh
    FROM prepared
    GROUP BY kwh_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM prepared s
    JOIN kwh_tier_stats g ON g.kwh_tier = s.kwh_tier
    WHERE s.kwh = g.max_kwh
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_voltage,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM compared s
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at_2
    FROM with_stats s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, quality_len, kwh_tier, kwh
    FROM summarized_rows
) src
PIVOT (
    MIN(kwh)
    FOR kwh_tier IN ('large', 'medium', 'small')
)
