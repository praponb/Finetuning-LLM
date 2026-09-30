WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
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
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(energy_cost * 0.9, 2) AS energy_cost_adj,
        FLOOR(energy_cost / 250) * 250 AS energy_cost_bucket
    FROM matched
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM derived s
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    zone,
    rate,
    energy_cost,
    energy_cost_adj,
    energy_cost_bucket,
    reading_count,
    reading_count_2
FROM with_counts
