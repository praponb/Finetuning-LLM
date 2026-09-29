WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(kwh - 52.5) AS kwh_distance
    FROM meter_readings
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        kwh_distance,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST), 3) AS read_at_cume_dist,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        SUM(kwh) OVER (ORDER BY kwh_distance DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior,
        ROUND(kwh_distance - AVG(kwh_distance) OVER (PARTITION BY site_id), 2) AS kwh_distance_vs_site_id_avg,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY MONTH(read_at) ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        LEAD(kwh_distance, 3, 0) OVER w_all_by_kwh_distance AS next3_kwh_distance,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage,
        MAX(kwh_distance) OVER (PARTITION BY site_id) AS site_id_max_kwh_distance,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        ROUND(kwh_distance / NULLIF(SUM(kwh_distance) OVER w_site_id, 0), 4) AS kwh_distance_share_of_site_id,
        DENSE_RANK() OVER w_site_id_by_kwh_distance AS kwh_distance_dense_rank,
        ROW_NUMBER() OVER w_read_at_month_by_voltage AS voltage_seq
    FROM filtered
    WINDOW w_site_id AS (PARTITION BY site_id), w_all_by_kwh_distance AS (ORDER BY kwh_distance DESC, id), w_site_id_by_kwh_distance AS (PARTITION BY site_id ORDER BY kwh_distance DESC, id), w_read_at_month_by_voltage AS (PARTITION BY MONTH(read_at) ORDER BY voltage, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(site_id_max_kwh_distance * 1.5, 2) AS site_id_max_kwh_distance_adj
    FROM windowed
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        kwh_distance,
        read_at_cume_dist,
        kwh_distance_vs_site_id_avg,
        last_known_voltage,
        next3_kwh_distance,
        site_id_min_voltage,
        site_id_max_kwh_distance,
        kwh_distance_share_of_site_id,
        kwh_distance_dense_rank,
        voltage_seq,
        site_id_max_kwh_distance_adj,
        MAX(kwh_distance_dense_rank) OVER (PARTITION BY kwh_distance_dense_rank ORDER BY next3_kwh_distance DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_distance_dense_rank_max_6_prior,
        ROUND(voltage_seq / NULLIF(SUM(voltage_seq) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS voltage_seq_share_of_read_at_year,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY YEAR(read_at))) AS distinct_site_id_in_read_at_year,
        LEAD(kwh_distance, 3) OVER (ORDER BY site_id_max_kwh_distance DESC, id) AS next3_kwh_distance_2
    FROM scored
)
-- Final projection
SELECT
    id,
    kwh,
    kwh_distance,
    read_at_cume_dist,
    kwh_distance_vs_site_id_avg,
    last_known_voltage,
    next3_kwh_distance,
    site_id_max_kwh_distance,
    kwh_distance_dense_rank,
    voltage_seq_share_of_read_at_year,
    distinct_site_id_in_read_at_year,
    next3_kwh_distance_2,
    LEAST(read_at_cume_dist, kwh_distance_dense_rank) AS least_read_at_cume_dist_kwh_distance_dense_rank
FROM with_trends
ORDER BY last_known_voltage DESC NULLS FIRST
