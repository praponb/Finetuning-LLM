WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage < 240.2
      AND site_id IS NOT NULL
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM scoped s
),
grouped AS (
    -- Group-level totals
    SELECT
        site_id,
        MIN(first_reading_read_at) AS first_first_reading_read_at,
        ROUND(100 * COUNT(CASE WHEN quality NOT IN ('EST', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_est_bad,
        COUNT(*) AS cnt
    FROM summarized_rows
    GROUP BY site_id
    HAVING COUNT(*) >= 2
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.first_first_reading_read_at,
        s.pct_quality_not_in_est_bad,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM grouped s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
with_details_reading AS (
    -- Attach reading attributes
    SELECT
        s.site_id,
        s.pct_quality_not_in_est_bad,
        s.reading_id,
        s.reading_voltage,
        s.reading_kwh,
        s.reading_quality,
        m.read_at AS reading_read_at_2,
        m.site_id AS reading_site_id,
        m.quality AS reading_quality_2
    FROM exploded_detail_readings s
    JOIN meter_readings m ON m.id = s.reading_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(reading_read_at_2, 'yyyy-MM-dd HH:00') AS reading_read_at_2_label
    FROM with_details_reading
)
SELECT *
FROM (
    SELECT reading_site_id, reading_quality, reading_voltage
    FROM enriched
) src
PIVOT (
    MAX(reading_voltage)
    FOR reading_quality IN ('BAD', 'OK', 'EST')
)
ORDER BY reading_site_id
