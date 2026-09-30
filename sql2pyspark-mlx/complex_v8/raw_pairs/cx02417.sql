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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at
    FROM covered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
site_id_stats AS (
    -- Max energy_cost per site_id
    SELECT site_id, MAX(energy_cost) AS max_energy_cost
    FROM with_counts
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_energy_cost,
        ROUND(s.energy_cost - g.max_energy_cost, 2) AS energy_cost_minus_max
    FROM with_counts s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.energy_cost = g.max_energy_cost
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost,
    last_reading_read_at,
    max_energy_cost,
    energy_cost_minus_max,
    DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
    LENGTH(TRIM(zone)) AS zone_len
FROM above_average
WHERE voltage < 240.1
ORDER BY energy_cost DESC NULLS FIRST, days_since_read_at, id
LIMIT 20
