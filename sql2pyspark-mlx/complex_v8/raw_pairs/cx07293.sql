WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 09:00:00'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(voltage) AS min_reading_voltage,
        MIN(kwh) AS min_reading_kwh,
        SUM(voltage) AS total_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        m.min_reading_kwh,
        m.total_reading_voltage
    FROM base s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(min_reading_voltage / NULLIF(SUM(min_reading_voltage) OVER w_read_at_week_month_site_id, 0), 4) AS min_reading_voltage_share_of_read_at_week_month_,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_week), site_id)) AS distinct_quality_in_read_at_week_month_site_id,
        NTH_VALUE(total_reading_voltage, 2) OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_reading_voltage,
        NTH_VALUE(total_reading_voltage, 2) OVER (PARTITION BY quality ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_reading_voltage_2,
        min_reading_voltage - LAG(min_reading_voltage) OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id) AS min_reading_voltage_change,
        COUNT(*) OVER w_read_at_week_month_site_id AS rows_in_read_at_week_month_site_id
    FROM screened
    WINDOW w_read_at_week_month_site_id AS (PARTITION BY DATE_TRUNC('MONTH', read_at_week), site_id)
),
grouped AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at) AS read_at_day,
        CASE WHEN min_reading_kwh >= 72.0 THEN 'large' WHEN min_reading_kwh >= 42.0 THEN 'medium' ELSE 'small' END AS min_reading_kwh_tier,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        SUM(second_total_reading_voltage) FILTER (WHERE quality = 'OK') AS second_total_reading_voltage_quality_ok,
        SUM(voltage) AS total_voltage,
        COUNT(*) AS cnt,
        MIN(rows_in_read_at_week_month_site_id) AS min_rows_in_read_at_week_month_site_id,
        COUNT(DISTINCT id) AS distinct_ids
    FROM sequenced
    GROUP BY TO_DATE(read_at), CASE WHEN min_reading_kwh >= 72.0 THEN 'large' WHEN min_reading_kwh >= 42.0 THEN 'medium' ELSE 'small' END
)
SELECT
    read_at_day,
    min_reading_kwh_tier,
    total_voltage,
    min_rows_in_read_at_week_month_site_id,
    ABS(cnt - 8) AS cnt_distance
FROM grouped
ORDER BY min_reading_kwh_tier DESC, read_at_day
