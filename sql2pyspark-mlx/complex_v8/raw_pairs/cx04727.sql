WITH in_window AS (
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
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM in_window s
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.energy_cost BETWEEN 180.0 AND 765.0
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY zone), 2) AS zone_avg_kwh,
        rate - LAG(rate) OVER w_zone_site_id_by_read_at AS rate_change,
        ROW_NUMBER() OVER w_zone_site_id_by_read_at AS read_at_seq,
        ROUND(PERCENT_RANK() OVER w_zone_site_id_by_read_at_ties, 3) AS read_at_percent_rank,
        SUM(rate) OVER (PARTITION BY zone) AS zone_sum_rate,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY zone, site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage
    FROM qualified
    WINDOW w_zone_site_id_by_read_at AS (PARTITION BY zone, site_id ORDER BY read_at, id), w_zone_site_id_by_read_at_ties AS (PARTITION BY zone, site_id ORDER BY read_at)
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    zone,
    first_reading_read_at,
    zone_avg_kwh,
    rate_change,
    read_at_seq,
    read_at_percent_rank,
    zone_sum_rate,
    second_voltage,
    DATE_TRUNC('YEAR', last_reading_read_at) AS last_reading_read_at_year,
    TO_DATE(first_reading_read_at) AS first_reading_read_at_date
FROM with_windows
WHERE rate <= 7.6
ORDER BY last_reading_read_at_year DESC, id
