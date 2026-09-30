WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        CASE WHEN voltage >= 237.8 THEN 'A' WHEN voltage >= 221.4 THEN 'B' ELSE 'C' END AS voltage_tier,
        ROUND(kwh * 1.07, 2) AS kwh_adj
    FROM meter_readings
    WHERE voltage < 233.8
),
site_id_stats AS (
    -- Max kwh_per_voltage per site_id
    SELECT site_id, MAX(kwh_per_voltage) AS max_kwh_per_voltage
    FROM filtered_readings
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_kwh_per_voltage,
        ROUND(s.kwh_per_voltage - g.max_kwh_per_voltage, 2) AS kwh_per_voltage_minus_max
    FROM filtered_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh_per_voltage = g.max_kwh_per_voltage
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh_per_voltage_minus_max / NULLIF(SUM(kwh_per_voltage_minus_max) OVER (PARTITION BY quality), 0), 4) AS kwh_per_voltage_minus_max_share_of_quality,
        FIRST_VALUE(kwh) OVER (ORDER BY max_kwh_per_voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        SIZE(COLLECT_SET(voltage_tier) OVER w_quality) AS distinct_voltage_tier_in_quality
    FROM compared
    WINDOW w_quality AS (PARTITION BY quality)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY MONTH(read_at)), 0), 4) AS kwh_share_of_read_at_month,
        kwh_per_voltage - LAG(kwh_per_voltage) OVER (ORDER BY kwh_per_voltage_minus_max, id) AS kwh_per_voltage_change
    FROM windowed
)
-- Subtotals with CUBE
SELECT
    quality,
    distinct_voltage_tier_in_quality,
    voltage_tier,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    COUNT(*) FILTER (WHERE quality = 'OK') AS n_quality_ok
FROM with_trends
GROUP BY CUBE(quality, distinct_voltage_tier_in_quality, voltage_tier)
ORDER BY quality NULLS LAST, distinct_voltage_tier_in_quality NULLS LAST, voltage_tier NULLS LAST
