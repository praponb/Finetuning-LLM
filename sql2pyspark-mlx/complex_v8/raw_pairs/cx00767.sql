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
quality_stats AS (
    -- Max kwh per quality
    SELECT quality, MAX(kwh) AS max_kwh
    FROM covered
    GROUP BY quality
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM covered s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh = g.max_kwh
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(energy_cost) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS energy_cost_avg_6_prior,
        MAX(kwh) OVER (PARTITION BY quality ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far
    FROM compared
)
-- Subtotals with ROLLUP
SELECT
    quality,
    site_id,
    zone,
    COUNT(DISTINCT quality) AS distinct_qualitys,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(STDDEV(kwh_minus_max), 2) AS stddev_kwh_minus_max
FROM windowed
GROUP BY ROLLUP(quality, site_id, zone)
