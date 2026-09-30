WITH filtered_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_ADD(commissioned_on, 30) AS commissioned_on_plus_30d,
        DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week
    FROM sites
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at
    FROM filtered_sites s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on,
        commissioned_on_plus_30d,
        reading_count,
        first_reading_read_at,
        CONCAT_WS('-', tariff, zone) AS tariff_zone_label
    FROM summarized_rows
    WHERE installed_kw <= 284.0
)
SELECT *
FROM (
    SELECT reading_count, tariff, installed_kw
    FROM tagged
) src
PIVOT (
    SUM(installed_kw)
    FOR tariff IN ('FLAT', 'TOU', 'EV')
)
ORDER BY reading_count
