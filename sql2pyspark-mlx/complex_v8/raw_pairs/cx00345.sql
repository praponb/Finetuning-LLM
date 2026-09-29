WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        si.zone AS site_zone,
        si.site_name
    FROM covered s
    JOIN sites si ON si.id = s.site_id
    WHERE LOWER(si.site_name) LIKE '%ep%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_zone), 0), 4) AS voltage_share_of_site_zone,
        ROUND(voltage - AVG(voltage) OVER w_site_zone, 2) AS voltage_vs_site_zone_avg,
        MAX(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        SUM(voltage) OVER (PARTITION BY site_zone) AS site_zone_sum_voltage
    FROM joined_site
    WINDOW w_site_zone AS (PARTITION BY site_zone)
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM metrics s
)
SELECT
    site_zone,
    site_id,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN site_zone = 'Z4' THEN 1 ELSE 0 END) AS n_site_zone_z4
FROM profiled
GROUP BY GROUPING SETS ((site_zone, site_id), (site_zone), (site_id))
ORDER BY site_zone NULLS LAST, site_id NULLS LAST
