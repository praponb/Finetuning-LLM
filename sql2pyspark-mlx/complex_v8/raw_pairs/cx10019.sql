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
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        ROUND(kwh * 1.07, 2) AS kwh_adj
    FROM joined_ranges
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE kwh > (SELECT MIN(kwh) FROM enriched)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    quality,
    energy_cost,
    kwh_bucket,
    quality_ref,
    kwh_adj,
    LOWER(TRIM(zone)) AS zone_clean,
    ROUND(rate * 1.1, 2) AS rate_adj
FROM screened
ORDER BY id DESC, read_at DESC
