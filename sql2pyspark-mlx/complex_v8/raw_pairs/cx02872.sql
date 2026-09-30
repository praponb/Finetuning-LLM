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
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(energy_cost) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), zone ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS energy_cost_max_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), zone ORDER BY kwh), 3) AS kwh_percent_rank,
        FIRST_VALUE(energy_cost) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), zone ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_energy_cost,
        ROUND(PERCENT_RANK() OVER w_read_at_month_zone_by_kwh, 3) AS kwh_percent_rank_2
    FROM in_window
    WINDOW w_read_at_month_zone_by_kwh AS (PARTITION BY DATE_TRUNC('MONTH', read_at), zone ORDER BY kwh, id)
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh
    FROM metrics s
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM summarized_rows s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality NOT IN ('BAD', 'OK')
              AND m.kwh < 90.6
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, zone, avg_reading_kwh
    FROM qualified
) src
PIVOT (
    MAX(avg_reading_kwh)
    FOR zone IN ('Z2', 'Z3', 'Z4')
)
ORDER BY site_id
