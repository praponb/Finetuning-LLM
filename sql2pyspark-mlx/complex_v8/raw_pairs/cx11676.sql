WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DAYOFWEEK(read_at) AS read_at_dayofweek,
        CASE WHEN voltage >= 235.0 THEN 'large' WHEN voltage >= 222.1 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
      AND voltage < 236.7
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM filtered_readings s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff IN ('TOU', 'EV')
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        read_at_dayofweek - LAG(read_at_dayofweek) OVER (PARTITION BY site_id ORDER BY read_at_dayofweek ASC, id) AS read_at_dayofweek_change,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id), 3) AS read_at_cume_dist,
        MAX(read_at_dayofweek) OVER (PARTITION BY site_id) AS site_id_max_read_at_dayofweek,
        MIN(voltage) OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far
    FROM qualified
    WINDOW w_site_id_by_read_at_dayofweek AS (PARTITION BY site_id ORDER BY read_at_dayofweek ASC, id)
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM with_windows s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone NOT IN ('Z2', 'Z4')
)
-- Subtotals with CUBE
SELECT
    COALESCE(site_zone, 'ALL') AS site_zone,
    site_id,
    ROUND(SUM(read_at_dayofweek_change) / NULLIF(SUM(site_installed_kw), 0), 3) AS read_at_dayofweek_change_to_site_installed_kw_ra,
    ROUND(SUM(site_id_max_read_at_dayofweek) / NULLIF(SUM(read_at_dayofweek), 0), 3) AS site_id_max_read_at_dayofweek_to_read_at_dayofwe
FROM with_details_site
GROUP BY CUBE(site_zone, site_id)
