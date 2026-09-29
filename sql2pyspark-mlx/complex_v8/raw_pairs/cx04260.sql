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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        energy_cost,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        LEAD(energy_cost, 3) OVER (PARTITION BY zone, site_id ORDER BY read_at ASC, id) AS next3_energy_cost,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER (PARTITION BY site_id), 0), 4) AS energy_cost_share_of_site_id,
        LEAD(read_at) OVER (PARTITION BY zone, site_id ORDER BY read_at ASC, id) AS next_read_at
    FROM matched
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        energy_cost,
        next3_energy_cost,
        rows_in_site_id,
        energy_cost_share_of_site_id,
        ROUND(rows_in_site_id / NULLIF(kwh, 0), 3) AS rows_in_site_id_per_kwh,
        ABS(kwh - 43.1) AS kwh_distance
    FROM sequenced
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE rows_in_site_id_per_kwh > (SELECT MIN(rows_in_site_id_per_kwh) FROM scored)
)
SELECT
    id,
    read_at,
    energy_cost,
    next3_energy_cost,
    rows_in_site_id,
    energy_cost_share_of_site_id,
    kwh_distance
FROM screened
WHERE rows_in_site_id_per_kwh <= 9.0
