WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY read_at_hour DESC), 3) AS read_at_hour_percent_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        SUM(kwh) OVER w_site_id AS site_id_sum_kwh,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        COUNT(*) OVER (PARTITION BY YEAR(read_at), DATE_TRUNC('MONTH', read_at_hour) ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id
    FROM prepared_readings
    WINDOW w_site_id AS (PARTITION BY site_id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN site_id_avg_voltage >= 234.5 THEN 'A' WHEN site_id_avg_voltage >= 222.2 THEN 'B' ELSE 'C' END AS site_id_avg_voltage_tier,
        DATEDIFF(DATE '2025-06-30', read_at_hour) AS days_since_read_at_hour
    FROM with_trends
    WHERE site_id >= 2
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 222.3
              AND TO_DATE(m.read_at) < DATE '2025-02-18'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, distinct_quality_in_site_id, days_since_read_at_hour
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (distinct_quality_in_site_id AS distinct_quality_in_site_id_v, days_since_read_at_hour AS m_days_since_read_at_hour)
) unp
ORDER BY id, field
