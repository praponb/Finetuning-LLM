WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE voltage < 238.8
      AND kwh <= 101.2
),
site_id_stats AS (
    -- Median kwh per site_id
    SELECT site_id, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM filtered_readings
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM filtered_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh >= g.median_kwh
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        median_kwh,
        kwh_minus_median,
        ROUND(median_kwh * 1.1, 2) AS median_kwh_adj
    FROM outliers
),
site_id_stats_2 AS (
    -- Max median_kwh_adj per site_id
    SELECT site_id, MAX(median_kwh_adj) AS max_median_kwh_adj
    FROM annotated
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_median_kwh_adj,
        ROUND(s.median_kwh_adj - g.max_median_kwh_adj, 2) AS median_kwh_adj_minus_max
    FROM annotated s
    JOIN site_id_stats_2 g ON g.site_id = s.site_id
    WHERE s.median_kwh_adj = g.max_median_kwh_adj
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage
    FROM vs_group s
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        CASE WHEN kwh >= 80.8 THEN 'HIGH' WHEN kwh >= 37.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        TO_DATE(read_at) AS read_at_day,
        MIN(avg_reading_voltage) AS min_avg_reading_voltage,
        MAX(avg_reading_voltage) AS max_avg_reading_voltage,
        MIN(max_median_kwh_adj) AS min_max_median_kwh_adj
    FROM with_counts
    WHERE median_kwh_adj >= 63.0
    GROUP BY 1, 2, 3
),
line_level AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.kwh_tier,
        s.read_at_day,
        s.min_avg_reading_voltage,
        s.max_avg_reading_voltage,
        s.min_max_median_kwh_adj,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM summary s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    site_id,
    kwh_tier,
    read_at_day,
    min_avg_reading_voltage,
    max_avg_reading_voltage,
    min_max_median_kwh_adj,
    reading_id,
    reading_read_at,
    reading_voltage,
    reading_kwh
FROM line_level
ORDER BY min_avg_reading_voltage DESC
