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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
site_id_stats AS (
    -- Max rate per site_id
    SELECT site_id, MAX(rate) AS max_rate
    FROM joined_ranges
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rate, ROUND(s.rate - g.max_rate, 2) AS rate_minus_max
    FROM joined_ranges s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.rate = g.max_rate
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    rate,
    max_rate,
    rate_minus_max
FROM above_average
WHERE voltage < 237.5
ORDER BY rate, zone, id
LIMIT 5
