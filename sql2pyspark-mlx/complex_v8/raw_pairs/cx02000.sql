WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
      AND quality IN ('EST', 'BAD')
),
latest_per_key AS (
    -- Keep the earliest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh, id) AS rn
        FROM scoped_readings
    ) ranked
    WHERE rn = 1
),
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage
    FROM latest_per_key
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM latest_per_key s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        avg_voltage,
        voltage_minus_avg,
        MAX(avg_voltage) OVER w_site_id_read_at_month AS site_id_read_at_month_max_avg_voltage,
        SUM(voltage_minus_avg) OVER (ORDER BY voltage_minus_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage_minus_avg,
        COUNT(*) OVER (PARTITION BY site_id, MONTH(read_at)) AS rows_in_site_id_read_at_month,
        ROUND(avg_voltage / NULLIF(SUM(avg_voltage) OVER w_site_id_read_at_month, 0), 4) AS avg_voltage_share_of_site_id_read_at_month,
        ROUND(AVG(voltage_minus_avg) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_minus_avg_avg_2,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY YEAR(read_at), site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at
    FROM outliers
    WINDOW w_site_id_read_at_month AS (PARTITION BY site_id, MONTH(read_at))
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.voltage_minus_avg,
        s.site_id_read_at_month_max_avg_voltage,
        s.avg_voltage_share_of_site_id_read_at_month,
        s.voltage_minus_avg_avg_2,
        s.last_known_read_at,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.site_id_read_at_month_max_avg_voltage,
        s.avg_voltage_share_of_site_id_read_at_month,
        s.voltage_minus_avg_avg_2,
        s.last_known_read_at,
        s.site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone_2
    FROM with_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    site_id_read_at_month_max_avg_voltage,
    avg_voltage_share_of_site_id_read_at_month,
    voltage_minus_avg_avg_2,
    last_known_read_at,
    site_installed_kw,
    LENGTH(TRIM(quality)) AS quality_len,
    LENGTH(TRIM(quality)) AS quality_len_2
FROM with_parent
ORDER BY quality_len DESC, last_known_read_at DESC
