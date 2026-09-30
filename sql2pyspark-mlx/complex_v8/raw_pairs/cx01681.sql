WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.5, 2) AS voltage_adj,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE kwh <= 81.3
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        MIN(voltage) AS min_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        m.min_reading_voltage
    FROM cleaned s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 241.8
        )
),
unique_rows AS (
    -- Keep the earliest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_adj,
        days_since_read_at,
        first_reading_read_at,
        min_reading_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY voltage_adj, id) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        quality,
        DATE_TRUNC('MONTH', first_reading_read_at) AS first_reading_read_at_month,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        ROUND(SUM(min_reading_voltage), 2) AS total_min_reading_voltage
    FROM unique_rows
    WHERE kwh < 79.4
    GROUP BY 1, 2, 3
    HAVING SUM(voltage) > 220.7
)
SELECT quality, first_reading_read_at_month, read_at_ym
FROM rollup_base
ORDER BY first_reading_read_at_month DESC, quality, read_at_ym
LIMIT 10
