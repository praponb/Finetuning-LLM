WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE voltage >= 221.4
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage_minus_kwh DESC) AS voltage_minus_kwh_rank,
        ROUND(voltage_minus_kwh / NULLIF(SUM(voltage_minus_kwh) OVER (PARTITION BY quality), 0), 4) AS voltage_minus_kwh_share_of_quality,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage,
        DENSE_RANK() OVER w_all_by_voltage_minus_kwh AS voltage_minus_kwh_dense_rank,
        LAST_VALUE(voltage_minus_kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_minus_kwh,
        LEAD(voltage_minus_kwh) OVER (PARTITION BY site_id ORDER BY voltage NULLS LAST, id) AS next_voltage_minus_kwh,
        voltage - LAG(voltage) OVER (ORDER BY voltage_minus_kwh, id) AS voltage_change,
        LAG(kwh) OVER (PARTITION BY quality, site_id ORDER BY voltage DESC, id) AS prev_kwh,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY voltage NULLS LAST, id) AS voltage_dense_rank,
        DENSE_RANK() OVER (ORDER BY voltage_minus_kwh) AS voltage_minus_kwh_dense_rank_2,
        LEAD(voltage_minus_kwh, 2) OVER (PARTITION BY quality, site_id ORDER BY voltage DESC, id) AS next2_voltage_minus_kwh
    FROM valid_readings
    WINDOW w_all_by_voltage_minus_kwh AS (ORDER BY voltage_minus_kwh ASC NULLS LAST, id), w_site_id AS (PARTITION BY site_id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ABS(voltage_change - -24.5) AS voltage_change_distance,
        ROUND(site_id_min_voltage / NULLIF(prev_kwh, 0), 3) AS site_id_min_voltage_per_prev_kwh
    FROM windowed
    WHERE site_id IS NOT NULL
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(voltage_change) OVER (PARTITION BY quality ORDER BY voltage_minus_kwh, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_change_sum_3_prior,
        LAST_VALUE(prev_kwh) OVER (ORDER BY rows_in_site_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_kwh,
        DENSE_RANK() OVER (ORDER BY rows_in_site_id DESC) AS rows_in_site_id_dense_rank,
        SUM(site_id_avg_voltage) OVER (PARTITION BY voltage_dense_rank, voltage_minus_kwh_dense_rank ORDER BY voltage_minus_kwh_share_of_quality, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS site_id_avg_voltage_sum_2,
        rows_in_site_id - LAG(rows_in_site_id) OVER w_voltage_dense_rank_voltage_m_2 AS rows_in_site_id_change,
        NTILE(10) OVER w_all_by_rows_in_site_id AS rows_in_site_id_decile,
        COUNT(*) OVER (PARTITION BY voltage_dense_rank) AS rows_in_voltage_dense_rank
    FROM annotated
    WINDOW w_voltage_dense_rank_voltage_m_2 AS (PARTITION BY voltage_dense_rank, voltage_minus_kwh_dense_rank ORDER BY voltage_minus_kwh_share_of_quality, id), w_all_by_rows_in_site_id AS (ORDER BY rows_in_site_id DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    site_id_avg_voltage,
    rows_in_site_id,
    voltage_minus_kwh_rank,
    voltage_minus_kwh_share_of_quality,
    site_id_min_voltage,
    voltage_minus_kwh_dense_rank,
    last_known_voltage_minus_kwh,
    next_voltage_minus_kwh,
    voltage_change,
    prev_kwh,
    voltage_dense_rank,
    voltage_minus_kwh_dense_rank_2,
    next2_voltage_minus_kwh,
    voltage_change_distance,
    site_id_min_voltage_per_prev_kwh,
    last_prev_kwh,
    site_id_avg_voltage_sum_2,
    rows_in_site_id_change,
    rows_in_site_id_decile,
    rows_in_voltage_dense_rank,
    ABS(voltage_dense_rank - 6) AS voltage_dense_rank_distance
FROM windowed_2
ORDER BY voltage_change_distance
