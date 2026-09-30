WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(voltage - 232.7) AS voltage_distance,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 09:00:00'
      AND site_id >= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY TO_DATE(read_at)), 0), 4) AS voltage_share_of_read_at_to_date,
        ROUND(voltage_distance / NULLIF(SUM(voltage_distance) OVER (PARTITION BY TO_DATE(read_at)), 0), 4) AS voltage_distance_share_of_read_at_to_date,
        DENSE_RANK() OVER w_all_by_read_at_ties AS read_at_dense_rank,
        MIN(voltage_distance) OVER (PARTITION BY quality_len, site_id) AS quality_len_site_id_min_voltage_distance,
        ROUND(PERCENT_RANK() OVER w_quality_len_read_at_month_by, 3) AS voltage_percent_rank,
        FIRST_VALUE(quality_len) IGNORE NULLS OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quality_len,
        LEAD(read_at, 2) OVER (PARTITION BY quality_len, DATE_TRUNC('MONTH', read_at) ORDER BY voltage ASC NULLS LAST, id) AS next2_read_at,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality_len, site_id), 0), 4) AS voltage_share_of_quality_len_site_id,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id))) / 60, 1) AS minutes_since_prev_read_at,
        NTH_VALUE(voltage_distance, 2) OVER (PARTITION BY quality_len, site_id ORDER BY voltage_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_distance,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY TO_DATE(read_at)), 2) AS kwh_vs_read_at_to_date_avg,
        kwh - LAG(kwh) OVER (PARTITION BY quality_len, DATE_TRUNC('MONTH', read_at) ORDER BY kwh ASC NULLS LAST, id) AS kwh_change,
        SUM(quality_len) OVER w_quality_len_site_id AS quality_len_site_id_sum_quality_len,
        NTH_VALUE(read_at, 2) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        RANK() OVER (ORDER BY read_at) AS read_at_rank
    FROM base_readings
    WINDOW w_all_by_read_at_ties AS (ORDER BY read_at), w_quality_len_read_at_month_by AS (PARTITION BY quality_len, DATE_TRUNC('MONTH', read_at) ORDER BY voltage ASC NULLS LAST, id), w_quality_len_site_id AS (PARTITION BY quality_len, site_id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE read_at_rank <= 5
      AND voltage_percent_rank BETWEEN 0.1 AND 0.7
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        quality_len,
        voltage_share_of_read_at_to_date,
        read_at_dense_rank,
        quality_len_site_id_min_voltage_distance,
        voltage_percent_rank,
        first_known_quality_len,
        next2_read_at,
        minutes_since_prev_read_at,
        kwh_vs_read_at_to_date_avg,
        kwh - LAG(kwh) OVER (PARTITION BY quality_len, MONTH(second_read_at) ORDER BY kwh, id) AS kwh_change_2,
        LAG(voltage, 3) OVER (PARTITION BY quality_len ORDER BY quality_len_site_id_min_voltage_distance NULLS LAST, id) AS prev3_voltage,
        NTH_VALUE(kwh_change, 2) OVER (PARTITION BY quality_len, MONTH(second_read_at) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_change,
        MAX(voltage_percent_rank) OVER (ORDER BY second_read_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_percent_rank_max_1_prior,
        LAG(kwh_vs_read_at_to_date_avg, 1, 0) OVER (PARTITION BY quality_len ORDER BY quality_len_site_id_min_voltage_distance NULLS LAST, id) AS prev_kwh_vs_read_at_to_date_avg
    FROM first_rows
)
-- Final projection
SELECT
    id,
    site_id,
    quality_len,
    voltage_share_of_read_at_to_date,
    read_at_dense_rank,
    voltage_percent_rank,
    first_known_quality_len,
    next2_read_at,
    minutes_since_prev_read_at,
    kwh_change_2,
    prev3_voltage,
    prev_kwh_vs_read_at_to_date_avg,
    CASE WHEN second_kwh_change >= 60.0 THEN 'HIGH' WHEN second_kwh_change >= -48.0 THEN 'MEDIUM' ELSE 'LOW' END AS second_kwh_change_tier
FROM windowed_2
WHERE minutes_since_prev_read_at BETWEEN 750.0 AND 4750.0
ORDER BY read_at_dense_rank DESC, first_known_quality_len
