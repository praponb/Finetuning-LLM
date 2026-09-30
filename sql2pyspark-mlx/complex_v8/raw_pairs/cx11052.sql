WITH joined_ranges AS (
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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY zone), 2) AS voltage_vs_zone_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY zone ORDER BY voltage ASC, id), 3) AS voltage_cume_dist
    FROM qualified
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY zone, MONTH(read_at) ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        NTILE(3) OVER (PARTITION BY zone ORDER BY energy_cost, id) AS energy_cost_tercile,
        ROW_NUMBER() OVER w_zone_read_at_month_by_read_a AS seq_no
    FROM sequenced
    WINDOW w_zone_read_at_month_by_read_a AS (PARTITION BY zone, DATE_TRUNC('MONTH', read_at) ORDER BY read_at NULLS LAST, id)
)
SELECT
    id,
    read_at,
    voltage,
    zone,
    rate,
    energy_cost,
    voltage_cume_dist,
    second_voltage,
    energy_cost_tercile
FROM windowed
ORDER BY rate DESC, voltage_cume_dist NULLS LAST
