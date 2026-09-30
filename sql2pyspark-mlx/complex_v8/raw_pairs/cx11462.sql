WITH matched AS (
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
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw > 66.5
              AND si.tariff = 'TOU'
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(kwh / NULLIF(energy_cost, 0), 3) AS kwh_per_energy_cost,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM qualified
),
zone_stats AS (
    -- Median rate per zone
    SELECT zone, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM annotated
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM annotated s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.rate >= g.median_rate
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    rate,
    energy_cost,
    kwh_per_energy_cost,
    quality_or_unknown,
    median_rate,
    LOWER(TRIM(quality_or_unknown)) AS quality_or_unknown_clean,
    CASE WHEN kwh_per_energy_cost >= 6.5 THEN 'A' WHEN kwh_per_energy_cost >= 3.0 THEN 'B' ELSE 'C' END AS kwh_per_energy_cost_tier
FROM outliers
WHERE quality_or_unknown <> 'BAD'
ORDER BY quality_or_unknown DESC, energy_cost
