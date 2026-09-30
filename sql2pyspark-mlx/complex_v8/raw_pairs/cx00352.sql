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
site_id_stats AS (
    -- Avg rate per site_id
    SELECT site_id, AVG(rate) AS avg_rate
    FROM covered
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM covered s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.rate > g.avg_rate
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, zone, rate
    FROM compared
) src
PIVOT (
    SUM(rate)
    FOR zone IN ('Z3', 'Z4', 'Z2')
)
ORDER BY read_at
