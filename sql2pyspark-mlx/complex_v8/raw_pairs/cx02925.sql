WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE site_id >= 5
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 237.8
              AND HOUR(m.read_at) BETWEEN 6 AND 12
        )
      AND s.kwh <= 81.1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_month), site_id), 2) AS kwh_vs_read_at_month_month_site_id_avg,
        SUM(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_sum_4
    FROM filtered
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_or_unknown,
        s.read_at_month,
        s.kwh_vs_read_at_month_month_site_id_avg,
        s.voltage_sum_4,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM metrics s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage_sum_4) OVER (PARTITION BY reading_id), 2) AS reading_id_avg_voltage_sum_4,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id, reading_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3
    FROM exploded_detail_readings
),
site_id_stats AS (
    -- Max reading_id_avg_voltage_sum_4 per site_id
    SELECT site_id, MAX(reading_id_avg_voltage_sum_4) AS max_reading_id_avg_voltage_sum_4
    FROM with_trends
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_reading_id_avg_voltage_sum_4,
        ROUND(s.reading_id_avg_voltage_sum_4 - g.max_reading_id_avg_voltage_sum_4, 2) AS reading_id_avg_voltage_sum_4_minus_max
    FROM with_trends s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.reading_id_avg_voltage_sum_4 = g.max_reading_id_avg_voltage_sum_4
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality_or_unknown,
    read_at_month,
    kwh_vs_read_at_month_month_site_id_avg,
    voltage_sum_4,
    reading_id,
    reading_read_at,
    reading_id_avg_voltage_sum_4,
    voltage_max_3,
    reading_id_avg_voltage_sum_4_minus_max,
    voltage - voltage_sum_4 AS voltage_minus_voltage_sum_4,
    SUBSTRING(TRIM(quality_or_unknown), 1, 2) AS quality_or_unknown_prefix
FROM compared
ORDER BY reading_id_avg_voltage_sum_4_minus_max DESC, quality_or_unknown, id, reading_id
LIMIT 20
