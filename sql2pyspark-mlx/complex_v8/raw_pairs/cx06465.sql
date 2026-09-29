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
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.rate,
        s.energy_cost,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM matched s
    JOIN meter_readings m ON m.site_id = s.site_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(energy_cost) OVER (ORDER BY reading_voltage DESC, id, reading_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS energy_cost_avg_6_prior,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER w_reading_id_site_id, 0), 4) AS energy_cost_share_of_reading_id_site_id,
        ROUND(AVG(kwh) OVER (PARTITION BY reading_id, site_id), 2) AS reading_id_site_id_avg_kwh,
        MAX(reading_voltage) OVER (ORDER BY reading_voltage DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_reading_voltage_so_far,
        ROUND(PERCENT_RANK() OVER (ORDER BY reading_voltage DESC), 3) AS reading_voltage_percent_rank
    FROM detail_rows_readings
    WINDOW w_reading_id_site_id AS (PARTITION BY reading_id, site_id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', reading_read_at) AS days_since_reading_read_at
    FROM windowed
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    rate,
    energy_cost,
    reading_voltage,
    reading_read_at,
    energy_cost_avg_6_prior,
    max_reading_voltage_so_far,
    days_since_reading_read_at,
    read_at + INTERVAL 2 HOURS AS read_at_plus_2h,
    QUARTER(reading_read_at) AS reading_read_at_quarter
FROM labeled
WHERE energy_cost_avg_6_prior >= 405.0
ORDER BY reading_read_at NULLS LAST
