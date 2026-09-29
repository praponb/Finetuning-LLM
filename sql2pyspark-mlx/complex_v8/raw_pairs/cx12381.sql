WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        COUNT(*) AS reading_count,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM valid_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        reading_count,
        reading_count_2,
        FIRST_VALUE(reading_count) IGNORE NULLS OVER (PARTITION BY reading_count ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reading_count,
        MAX(reading_count_2) OVER (PARTITION BY reading_count ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS reading_count_2_max_1_prior,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_read_at_week_month_site_id, 0), 4) AS voltage_share_of_read_at_week_month_site_id,
        LAST_VALUE(last_reading_read_at) OVER (PARTITION BY reading_count ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_reading_read_at
    FROM summarized_rows
    WINDOW w_read_at_week_month_site_id AS (PARTITION BY MONTH(read_at_week), site_id)
)
SELECT
    id,
    read_at,
    reading_count,
    reading_count_2,
    first_known_reading_count,
    reading_count_2_max_1_prior,
    voltage_share_of_read_at_week_month_site_id,
    last_last_reading_read_at,
    read_at + INTERVAL 1 DAYS AS read_at_plus_1d,
    ABS(reading_count_2 - 4) AS reading_count_2_distance
FROM metrics
ORDER BY reading_count DESC, voltage_share_of_read_at_week_month_site_id
