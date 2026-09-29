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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage > 222.1
              AND m.quality = 'BAD'
        )
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
zone_stats AS (
    -- Median kwh per zone
    SELECT zone, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM without_matches
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM without_matches s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.kwh >= g.median_kwh
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    zone,
    rate,
    energy_cost,
    kwh_minus_median,
    LOWER(TRIM(quality)) AS quality_clean,
    CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
FROM outliers
ORDER BY quality
