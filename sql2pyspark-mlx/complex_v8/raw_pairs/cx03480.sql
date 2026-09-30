WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 13:00:00'
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.installed_kw AS site_installed_kw
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_details_site
    WHERE kwh > (SELECT AVG(kwh) FROM with_details_site)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        site_installed_kw,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_read_at_to_date, 0), 4) AS voltage_share_of_site_id_read_at_to_date,
        MAX(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        COUNT(*) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS rows_in_site_id_read_at_to_date,
        LAG(kwh, 2) OVER (ORDER BY voltage DESC, id) AS prev2_kwh
    FROM screened
    WINDOW w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at))
)
-- Final projection
SELECT
    id,
    read_at,
    site_installed_kw,
    voltage_share_of_site_id_read_at_to_date,
    max_kwh_so_far,
    rows_in_site_id_read_at_to_date,
    read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
FROM with_trends
WHERE TO_DATE(read_at) < DATE '2025-03-02'
ORDER BY id NULLS LAST, read_at
