WITH joined_ranges AS (
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
    -- Join sites
    SELECT s.*, si.zone AS site_zone, si.site_name
    FROM joined_ranges s
    LEFT JOIN sites si ON si.id = s.site_id
),
summary AS (
    -- Group-level totals
    SELECT
        site_zone,
        zone,
        MIN(read_at) AS first_read_at,
        COUNT(*) FILTER (WHERE zone = 'Z3') AS n_zone_z3
    FROM joined_site
    WHERE zone IN ('Z4', 'Z2')
    GROUP BY site_zone, zone
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE n_zone_z3 > (SELECT AVG(n_zone_z3) FROM summary)
)
SELECT
    site_zone,
    zone,
    first_read_at,
    n_zone_z3
FROM filtered
