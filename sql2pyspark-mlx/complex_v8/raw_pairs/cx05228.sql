WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS kwh_vs_read_at_year_site_id_avg,
        LAG(voltage, 2) OVER (ORDER BY voltage, id) AS prev2_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY voltage), 3) AS voltage_cume_dist,
        LAG(voltage, 1, 0) OVER w_all_by_kwh AS prev_voltage,
        SUM(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_sum_1_prior,
        ROUND(AVG(voltage) OVER (ORDER BY kwh NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        COUNT(*) OVER (PARTITION BY YEAR(read_at), site_id) AS rows_in_read_at_year_site_id,
        ROW_NUMBER() OVER (ORDER BY voltage DESC, id) AS rn,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh
    FROM filtered_readings
    WINDOW w_all_by_kwh AS (ORDER BY kwh NULLS LAST, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh_vs_read_at_year_site_id_avg,
        prev2_voltage,
        voltage_cume_dist,
        prev_voltage,
        voltage_avg_2_prior,
        rows_in_read_at_year_site_id,
        rn,
        YEAR(read_at) AS read_at_year,
        COALESCE(prev_voltage, 0.0) AS prev_voltage_filled,
        ROUND(rn / NULLIF(prev2_voltage, 0), 3) AS rn_per_prev2_voltage
    FROM with_windows
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(prev2_voltage / NULLIF(SUM(prev2_voltage) OVER (PARTITION BY rn), 0), 4) AS prev2_voltage_share_of_rn,
        LAG(prev_voltage_filled, 2, 0) OVER (PARTITION BY site_id, rn ORDER BY voltage_cume_dist DESC, id) AS prev2_prev_voltage_filled,
        COUNT(*) OVER (PARTITION BY rn) AS rows_in_rn,
        MAX(voltage_avg_2_prior) OVER (ORDER BY rn ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_avg_2_prior_max_7,
        COUNT(*) OVER (PARTITION BY site_id, rn ORDER BY voltage_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, rn ORDER BY voltage_cume_dist DESC, id), 3) AS voltage_cume_dist_cume_dist,
        rn_per_prev2_voltage - LAG(rn_per_prev2_voltage) OVER (ORDER BY rn ASC, id) AS rn_per_prev2_voltage_change,
        NTILE(10) OVER (ORDER BY rn ASC, id) AS rn_decile
    FROM annotated
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        read_at_year,
        rn_decile,
        site_id,
        MIN(read_at) AS first_read_at
    FROM windowed
    WHERE kwh_vs_read_at_year_site_id_avg BETWEEN -72.0 AND 84.0
    GROUP BY read_at_year, rn_decile, site_id
)
-- Final projection
SELECT
    read_at_year,
    rn_decile,
    site_id,
    first_read_at,
    rn_decile - read_at_year AS rn_decile_minus_read_at_year
FROM summary
ORDER BY rn_decile_minus_read_at_year DESC
