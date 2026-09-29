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
zone_stats AS (
    -- Max rate per zone
    SELECT zone, MAX(rate) AS max_rate
    FROM joined_ranges
    GROUP BY zone
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rate, ROUND(s.rate - g.max_rate, 2) AS rate_minus_max
    FROM joined_ranges s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.rate = g.max_rate
),
site_id_stats AS (
    -- Max energy_cost per site_id
    SELECT site_id, MAX(energy_cost) AS max_energy_cost
    FROM compared
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_energy_cost,
        ROUND(s.energy_cost - g.max_energy_cost, 2) AS energy_cost_minus_max
    FROM compared s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.energy_cost = g.max_energy_cost
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM outliers s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    kwh,
    zone,
    rate,
    energy_cost,
    max_energy_cost,
    energy_cost_minus_max,
    first_reading_read_at
FROM with_counts
ORDER BY rate DESC, energy_cost_minus_max NULLS LAST
