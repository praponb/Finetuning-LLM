WITH matched AS (
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
site_id_stats AS (
    -- Max voltage per site_id
    SELECT site_id, MAX(voltage) AS max_voltage
    FROM matched
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM matched s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage = g.max_voltage
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        SUM(CASE WHEN zone = 'Z3' THEN 1 ELSE 0 END) AS n_zone_z3,
        COUNT(*) FILTER (WHERE zone = 'Z4') AS n_zone_z4,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT zone) AS distinct_zones
    FROM above_average
    WHERE voltage_minus_max >= -122.5
    GROUP BY TO_DATE(read_at), site_id
),
annotated AS (
    -- Label and score rows
    SELECT
        read_at_day,
        site_id,
        n_zone_z3,
        n_zone_z4,
        n_rows,
        distinct_zones,
        ABS(n_zone_z4 - 4) AS n_zone_z4_distance
    FROM stats
)
-- Final projection
SELECT
    read_at_day,
    site_id,
    n_zone_z4,
    n_rows,
    distinct_zones,
    n_zone_z4_distance,
    GREATEST(distinct_zones, n_zone_z3) AS greatest_distinct_zones_n_zone_z3,
    DATE_FORMAT(read_at_day, 'yyyy-MM-dd') AS read_at_day_label
FROM annotated
ORDER BY site_id DESC NULLS FIRST, distinct_zones NULLS LAST, read_at_day
LIMIT 10
