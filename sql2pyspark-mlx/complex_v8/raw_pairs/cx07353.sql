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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(kwh - 43.1) AS kwh_distance,
        COALESCE(kwh, 0.0) AS kwh_filled,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM matched
    WHERE kwh > 16.5
)
SELECT
    id,
    site_id,
    read_at,
    quality,
    zone,
    rate,
    kwh_distance,
    kwh_filled,
    read_at_label,
    FLOOR(kwh_filled / 25) * 25 AS kwh_filled_bucket,
    COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
FROM scored
ORDER BY read_at_label DESC NULLS FIRST, read_at NULLS LAST, id
LIMIT 10
