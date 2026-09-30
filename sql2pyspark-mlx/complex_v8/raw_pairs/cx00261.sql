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
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        ABS(energy_cost - 450.0) AS energy_cost_distance,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM matched
    WHERE TO_DATE(read_at) < DATE '2025-03-08'
)
SELECT *
FROM (
    SELECT site_id, read_at, zone, kwh
    FROM derived
) src
PIVOT (
    AVG(kwh)
    FOR zone IN ('Z2', 'Z4', 'Z3')
)
ORDER BY site_id, read_at
