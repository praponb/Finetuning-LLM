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
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        rate,
        SUM(energy_cost) OVER (PARTITION BY site_id ORDER BY energy_cost DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS energy_cost_sum_2,
        ROUND(AVG(voltage) OVER (ORDER BY kwh, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY energy_cost DESC, id), 3) AS energy_cost_percent_rank,
        RANK() OVER (PARTITION BY site_id ORDER BY energy_cost DESC) AS energy_cost_rank
    FROM joined_ranges
),
unique_rows AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        rate,
        energy_cost_sum_2,
        voltage_avg_1_prior,
        energy_cost_percent_rank,
        energy_cost_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage_avg_1_prior DESC, id DESC) AS rn
        FROM with_trends
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY energy_cost_rank, id) AS energy_cost_rank_quintile,
        DENSE_RANK() OVER (ORDER BY energy_cost_rank) AS energy_cost_rank_dense_rank,
        MAX(energy_cost_percent_rank) OVER (ORDER BY energy_cost_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS energy_cost_percent_rank_max_3,
        DENSE_RANK() OVER w_all_by_energy_cost_percent_r AS energy_cost_percent_rank_dense_rank,
        ROUND(AVG(energy_cost_rank) OVER (ORDER BY energy_cost_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS energy_cost_rank_avg_7
    FROM unique_rows
    WINDOW w_all_by_energy_cost_percent_r AS (ORDER BY energy_cost_percent_rank, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    energy_cost_percent_rank,
    energy_cost_rank,
    energy_cost_rank_dense_rank,
    energy_cost_percent_rank_max_3
FROM metrics
ORDER BY energy_cost_rank_dense_rank DESC
