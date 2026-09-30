WITH covered AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
zone_stats AS (
    -- Avg rate per zone
    SELECT zone, AVG(rate) AS avg_rate
    FROM covered
    GROUP BY zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM covered s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.rate > g.avg_rate
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    COALESCE(zone, 'ALL') AS zone,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING(site_id) AS is_total_site_id,
    COUNT(*) FILTER (WHERE zone IN ('Z2', 'Z3')) AS n_zone_in_z2_z3,
    MIN(read_at) AS first_read_at,
    COUNT(*) FILTER (WHERE quality IN ('BAD', 'OK')) AS n_quality_in_bad_ok
FROM above_average
GROUP BY GROUPING SETS ((site_id, zone, quality), (site_id), (zone), ())
ORDER BY site_id NULLS LAST, zone NULLS LAST, quality NULLS LAST
