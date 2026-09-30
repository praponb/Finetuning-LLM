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
unique_rows AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY rate DESC, id DESC) AS recency_rank
        FROM joined_ranges
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        zone,
        rate,
        RANK() OVER (ORDER BY rate, id) AS rate_rank,
        NTILE(4) OVER w_zone_by_rate AS rate_quartile,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        MIN(energy_cost) OVER (PARTITION BY site_id) AS site_id_min_energy_cost,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage
    FROM unique_rows
    WINDOW w_zone_by_rate AS (PARTITION BY zone ORDER BY rate ASC NULLS LAST, id), w_site_id AS (PARTITION BY site_id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        zone,
        rate,
        rate_rank,
        rate_quartile,
        kwh_vs_site_id_avg,
        site_id_min_energy_cost,
        ROUND(AVG(rate) OVER (PARTITION BY rate_rank, site_id ORDER BY site_id_avg_voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rate_avg_4,
        LEAD(rate, 3) OVER (ORDER BY rate_rank, id) AS next3_rate,
        COUNT(*) OVER (PARTITION BY rate_rank, zone) AS rows_in_rate_rank_zone,
        LEAD(read_at) OVER (PARTITION BY site_id, zone ORDER BY rate_quartile ASC, id) AS next_read_at,
        ROUND(AVG(rate_quartile) OVER (PARTITION BY rate_rank, zone), 2) AS rate_rank_zone_avg_rate_quartile
    FROM sequenced
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    zone,
    rate,
    rate_rank,
    rate_quartile,
    kwh_vs_site_id_avg,
    site_id_min_energy_cost,
    rate_avg_4,
    next3_rate,
    rows_in_rate_rank_zone,
    rate_rank_zone_avg_rate_quartile,
    ROUND(rate_avg_4 * 1.5, 2) AS rate_avg_4_adj
FROM metrics
