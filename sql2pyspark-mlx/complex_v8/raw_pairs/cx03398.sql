WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 9 AND 11
      AND voltage >= 220.2
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.least_kwh_voltage,
        si.site_name
    FROM valid s
    JOIN sites si ON si.id = s.site_id
    WHERE LENGTH(TRIM(si.site_name)) >= 8
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        least_kwh_voltage,
        ROUND(CUME_DIST() OVER w_all_by_voltage, 3) AS voltage_cume_dist,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        RANK() OVER (ORDER BY voltage, id) AS voltage_rank,
        NTILE(5) OVER w_all_by_voltage AS voltage_quintile,
        NTILE(5) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id) AS read_at_quintile,
        COUNT(*) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_details
    WINDOW w_all_by_voltage AS (ORDER BY voltage, id)
)
SELECT
    id,
    read_at,
    kwh,
    voltage,
    least_kwh_voltage,
    voltage_cume_dist,
    voltage_rank,
    read_at_quintile,
    running_count
FROM windowed
ORDER BY kwh, id
LIMIT 5
