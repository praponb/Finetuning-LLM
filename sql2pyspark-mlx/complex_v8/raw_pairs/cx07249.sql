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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(voltage) AS min_reading_voltage,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        m.first_reading_read_at
    FROM in_window s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_site_id_by_read_at, 3) AS read_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY read_at, id) AS read_at_dense_rank,
        LEAD(read_at, 3) OVER w_site_id_by_read_at AS next3_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        LEAD(rate, 2, 0) OVER w_site_id_by_read_at AS next2_rate,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_max_6_prior
    FROM with_stats
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        zone,
        energy_cost,
        min_reading_voltage,
        first_reading_read_at,
        read_at_percent_rank,
        read_at_dense_rank,
        next3_read_at,
        kwh_avg_3,
        voltage_max_6_prior,
        ROW_NUMBER() OVER w_first_reading_read_at_month AS rn,
        FIRST_VALUE(next2_rate) OVER (PARTITION BY MONTH(first_reading_read_at), site_id ORDER BY read_at_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next2_rate,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(first_reading_read_at), site_id ORDER BY read_at_dense_rank DESC, id), 3) AS read_at_dense_rank_cume_dist
    FROM sequenced
    WINDOW w_first_reading_read_at_month AS (PARTITION BY MONTH(first_reading_read_at), site_id ORDER BY read_at_dense_rank DESC, id)
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    zone,
    energy_cost,
    min_reading_voltage,
    first_reading_read_at,
    read_at_percent_rank,
    read_at_dense_rank,
    kwh_avg_3,
    first_next2_rate,
    read_at_dense_rank_cume_dist
FROM with_trends
WHERE MONTH(first_reading_read_at) IN (1, 3)
ORDER BY first_reading_read_at
