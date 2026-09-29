WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
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
unique_rows AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY rate DESC, id DESC) AS rn
        FROM joined_ranges
    ) ranked
    WHERE rn = 1
),
zone_stats AS (
    -- Avg kwh per zone
    SELECT zone, AVG(kwh) AS avg_kwh
    FROM unique_rows
    GROUP BY zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM unique_rows s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.kwh > g.avg_kwh
),
zone_stats_2 AS (
    -- Avg rate per zone
    SELECT zone, AVG(rate) AS avg_rate
    FROM above_average
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM above_average s
    JOIN zone_stats_2 g ON g.zone = s.zone
    WHERE s.rate > g.avg_rate
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    rate,
    energy_cost,
    avg_kwh,
    kwh_minus_avg
FROM outliers
ORDER BY read_at DESC NULLS FIRST
