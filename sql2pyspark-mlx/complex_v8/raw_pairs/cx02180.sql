WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'BAD')
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.tariff AS site_tariff
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(AVG(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        LAG(kwh) OVER (PARTITION BY site_tariff ORDER BY kwh, id) AS prev_kwh,
        NTILE(10) OVER w_site_tariff_by_kwh AS kwh_decile,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_id)) AS distinct_site_tariff_in_site_id,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh,
        FIRST_VALUE(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        MAX(kwh) OVER (PARTITION BY site_id, site_tariff ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_max_6_prior,
        FIRST_VALUE(read_at) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at
    FROM with_details
    WINDOW w_site_tariff_by_kwh AS (PARTITION BY site_tariff ORDER BY kwh, id)
),
stats AS (
    -- Summarise per group
    SELECT
        kwh_decile,
        DATE_FORMAT(last_known_read_at, 'yyyy-MM') AS last_known_read_at_ym,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        MIN(first_read_at) AS first_first_read_at,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    GROUP BY kwh_decile, DATE_FORMAT(last_known_read_at, 'yyyy-MM'), DATE_FORMAT(read_at, 'yyyy-MM')
)
-- Final projection
SELECT
    kwh_decile,
    last_known_read_at_ym,
    read_at_ym,
    first_first_read_at
FROM stats
ORDER BY read_at_ym
