WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE kwh >= 52.4
      AND quality = 'OK'
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 87.1
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        NTH_VALUE(read_at, 2) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh_per_voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        LAG(voltage) OVER w_all_by_kwh_per_voltage AS prev_voltage,
        SUM(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        SUM(voltage) OVER (ORDER BY kwh_per_voltage NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_sum_7
    FROM filtered
    WINDOW w_all_by_kwh_per_voltage AS (ORDER BY kwh_per_voltage NULLS LAST, id)
),
site_id_stats AS (
    -- Avg running_kwh per site_id
    SELECT site_id, AVG(running_kwh) AS avg_running_kwh
    FROM metrics
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_running_kwh,
        ROUND(s.running_kwh - g.avg_running_kwh, 2) AS running_kwh_minus_avg
    FROM metrics s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.running_kwh > g.avg_running_kwh
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        quality,
        kwh_share_of_site_id,
        last_known_read_at,
        prev_voltage,
        running_kwh,
        voltage_sum_7,
        running_kwh_minus_avg,
        SUM(kwh_share_of_site_id) OVER (PARTITION BY TO_DATE(second_read_at) ORDER BY last_known_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_share_of_site_id,
        LAG(avg_running_kwh) OVER w_second_read_at_to_date_by_la_2 AS prev_avg_running_kwh,
        MIN(voltage) OVER (PARTITION BY MONTH(second_read_at), quality) AS second_read_at_month_quality_min_voltage,
        voltage_sum_7 - LAG(voltage_sum_7) OVER (PARTITION BY site_id ORDER BY read_at, id) AS voltage_sum_7_change,
        LEAD(voltage, 2, 0) OVER (PARTITION BY quality ORDER BY last_known_read_at DESC, id) AS next2_voltage
    FROM compared
    WINDOW w_second_read_at_to_date_by_la_2 AS (PARTITION BY TO_DATE(second_read_at) ORDER BY last_known_read_at DESC, id)
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.kwh_share_of_site_id,
        s.last_known_read_at,
        s.voltage_sum_7,
        s.running_kwh_share_of_site_id,
        s.prev_avg_running_kwh,
        s.second_read_at_month_quality_min_voltage,
        s.voltage_sum_7_change,
        si.site_name
    FROM windowed s
    JOIN sites si ON si.id = s.site_id
    WHERE UPPER(TRIM(si.site_name)) LIKE 'M%'
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT COUNT(*) FILTER (WHERE m.quality IS NOT NULL) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at_2
    FROM joined_site s
)
-- Final projection
SELECT
    id,
    quality,
    last_known_read_at,
    voltage_sum_7,
    running_kwh_share_of_site_id,
    prev_avg_running_kwh,
    second_read_at_month_quality_min_voltage,
    voltage_sum_7_change,
    site_name,
    first_reading_read_at,
    reading_quality_count,
    first_reading_read_at_2
FROM summarized_rows
ORDER BY first_reading_read_at DESC, id
LIMIT 10
