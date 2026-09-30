WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        COALESCE(kwh, 0.0) AS kwh_filled,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE site_id >= 2
),
site_id_stats AS (
    -- Median kwh_filled per site_id
    SELECT site_id, PERCENTILE_APPROX(kwh_filled, 0.5) AS median_kwh_filled
    FROM filtered_readings
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_kwh_filled,
        ROUND(s.kwh_filled - g.median_kwh_filled, 2) AS kwh_filled_minus_median
    FROM filtered_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh_filled >= g.median_kwh_filled
)
SELECT id, measure, value
FROM (
    SELECT id, voltage, kwh_filled, voltage_per_kwh
    FROM above_average
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (voltage AS m_voltage, kwh_filled AS kwh_filled_v, voltage_per_kwh AS voltage_per_kwh_v)
) unp
ORDER BY id, measure
