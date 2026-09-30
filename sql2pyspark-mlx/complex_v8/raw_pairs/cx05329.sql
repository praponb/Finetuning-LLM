WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown_2
    FROM meter_readings
    WHERE quality = 'BAD'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(quality_or_unknown) OVER w_site_id) AS distinct_quality_or_unknown_in_site_id,
        MAX(voltage) OVER (PARTITION BY site_id) AS site_id_max_voltage,
        LAST_VALUE(voltage) OVER (PARTITION BY quality_or_unknown ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id
    FROM recent
    WINDOW w_site_id AS (PARTITION BY site_id)
),
site_id_stats AS (
    -- Avg voltage_share_of_site_id per site_id
    SELECT site_id, AVG(voltage_share_of_site_id) AS avg_voltage_share_of_site_id
    FROM windowed
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage_share_of_site_id,
        ROUND(s.voltage_share_of_site_id - g.avg_voltage_share_of_site_id, 2) AS voltage_share_of_site_id_minus_avg
    FROM windowed s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage_share_of_site_id > g.avg_voltage_share_of_site_id
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    quality,
    quality_or_unknown,
    quality_or_unknown_2,
    distinct_quality_or_unknown_in_site_id,
    site_id_max_voltage,
    last_voltage,
    voltage_share_of_site_id,
    rows_in_site_id,
    avg_voltage_share_of_site_id,
    voltage_share_of_site_id_minus_avg,
    CONCAT_WS('|', quality_or_unknown, quality) AS quality_or_unknown_quality_label,
    LOWER(REGEXP_REPLACE(TRIM(quality_or_unknown_2), ' ', '_')) AS quality_or_unknown_2_slug
FROM above_average
WHERE voltage >= 224.8
ORDER BY voltage_share_of_site_id
