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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw <= 284.0
),
zone_stats AS (
    -- Median rate per zone
    SELECT zone, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM combined_details_site
    GROUP BY zone
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM combined_details_site s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.rate >= g.median_rate
)
SELECT
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE zone = 'Z4') AS n_zone_z4,
    SUM(rate_minus_median) FILTER (WHERE quality IN ('EST', 'OK')) AS rate_minus_median_quality_in_est_ok
FROM vs_group
GROUP BY GROUPING SETS ((site_tariff, quality), (site_tariff), (quality), ())
