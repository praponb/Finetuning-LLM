WITH in_window AS (
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
scored AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(zone)) AS zone_clean,
        ABS(rate - 4.2) AS rate_distance,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM in_window
    WHERE zone <> 'Z2'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM scored
    WHERE rate < 7.6
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality,
    zone,
    rate,
    kwh_bucket,
    days_since_read_at,
    read_at + INTERVAL 7 DAYS AS read_at_plus_7d
FROM enriched
WHERE kwh_bucket BETWEEN 18 AND 84
ORDER BY zone DESC, kwh_bucket DESC
