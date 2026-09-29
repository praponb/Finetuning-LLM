WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 220.2 AND 236.8
      AND read_at >= TIMESTAMP '2025-01-13 15:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(kwh) OVER w_site_id AS site_id_sum_kwh,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage,
        voltage - LAG(voltage) OVER w_quality_by_kwh AS voltage_change,
        DENSE_RANK() OVER w_quality_by_kwh_ties AS kwh_dense_rank,
        RANK() OVER (PARTITION BY quality ORDER BY kwh) AS kwh_rank
    FROM cleaned
    WINDOW w_site_id AS (PARTITION BY site_id), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh, id), w_quality_by_kwh_ties AS (PARTITION BY quality ORDER BY kwh)
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND LOWER(si.site_name) LIKE '%no%'
              AND si.installed_kw < 373.3
        )
),
derived AS (
    -- Label and score rows
    SELECT *, TO_DATE(read_at) AS read_at_date
    FROM filtered
    WHERE quality NOT IN ('BAD', 'EST')
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM derived s
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        reading_count_2,
        DATE_TRUNC('MONTH', read_at_date) AS read_at_date_month,
        CASE WHEN site_id_sum_kwh >= 720.0 THEN 'large' WHEN site_id_sum_kwh >= 240.0 THEN 'medium' ELSE 'small' END AS site_id_sum_kwh_tier,
        COUNT(*) AS n_rows,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage_change), 0), 3) AS kwh_to_voltage_change_ratio,
        ROUND(SUM(voltage) / NULLIF(SUM(reading_count), 0), 3) AS voltage_to_reading_count_ratio
    FROM summarized_rows
    WHERE voltage_change >= -49.0
    GROUP BY reading_count_2, DATE_TRUNC('MONTH', read_at_date), CASE WHEN site_id_sum_kwh >= 720.0 THEN 'large' WHEN site_id_sum_kwh >= 240.0 THEN 'medium' ELSE 'small' END
)
SELECT
    reading_count_2,
    read_at_date_month,
    site_id_sum_kwh_tier,
    n_rows,
    voltage_to_reading_count_ratio
FROM totals
ORDER BY read_at_date_month, site_id_sum_kwh_tier
