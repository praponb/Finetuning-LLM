WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY quality, YEAR(read_at) ORDER BY read_at ASC, id) AS seq_no,
        ROUND(kwh - AVG(kwh) OVER w_read_at_month, 2) AS kwh_vs_read_at_month_avg,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY MONTH(read_at))) AS distinct_quality_in_read_at_month
    FROM cleaned
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at))
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 221.4
        )
),
with_details_site AS (
    -- Attach site attributes
    SELECT s.*, si.site_name, si.zone AS site_zone
    FROM screened s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at
    FROM with_details_site s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT id, field, value
FROM (
    SELECT id, voltage, kwh_vs_read_at_month_avg, kwh
    FROM with_counts
) src
UNPIVOT (
    value FOR field IN (voltage AS m_voltage, kwh_vs_read_at_month_avg AS m_kwh_vs_read_at_month_avg, kwh AS m_kwh)
) unp
