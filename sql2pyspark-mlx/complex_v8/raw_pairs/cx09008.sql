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
labeled AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        quality,
        zone,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        GREATEST(rate, kwh) AS greatest_rate_kwh
    FROM matched
    WHERE site_id IS NOT NULL
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE greatest_rate_kwh > (SELECT AVG(greatest_rate_kwh) FROM labeled)
)
SELECT
    id,
    quality,
    zone,
    read_at_hour,
    greatest_rate_kwh,
    COALESCE(greatest_rate_kwh, 0.0) AS greatest_rate_kwh_filled,
    DATE_FORMAT(read_at_hour, 'yyyy-MM-dd HH:00') AS read_at_hour_label
FROM with_activity
WHERE days_since_read_at > 280
ORDER BY greatest_rate_kwh_filled DESC, read_at_hour DESC
