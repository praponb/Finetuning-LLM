WITH covered AS (
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
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        ROUND(CUME_DIST() OVER w_all_by_voltage, 3) AS voltage_cume_dist
    FROM covered
    WINDOW w_all_by_voltage AS (ORDER BY voltage, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ABS(kwh_share_of_site_id - 0.6) AS kwh_share_of_site_id_distance,
        ROUND(voltage_cume_dist * 0.25, 2) AS voltage_cume_dist_adj
    FROM with_windows
    WHERE rate >= 4.2
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        rate,
        energy_cost,
        voltage_cume_dist,
        kwh_share_of_site_id_distance,
        voltage_cume_dist_adj,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY voltage ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        ROUND(rate - AVG(rate) OVER (PARTITION BY MONTH(read_at), zone), 2) AS rate_vs_read_at_month_zone_avg,
        ROUND(voltage_cume_dist_adj / NULLIF(SUM(voltage_cume_dist_adj) OVER (PARTITION BY MONTH(read_at), zone), 0), 4) AS voltage_cume_dist_adj_share_of_read_at_month_zon,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY voltage ASC) AS voltage_dense_rank,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage
    FROM labeled
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    energy_cost,
    voltage_cume_dist_adj,
    voltage_avg_2_prior,
    rate_vs_read_at_month_zone_avg,
    first_known_voltage
FROM with_windows_2
ORDER BY rate_vs_read_at_month_zone_avg NULLS LAST
