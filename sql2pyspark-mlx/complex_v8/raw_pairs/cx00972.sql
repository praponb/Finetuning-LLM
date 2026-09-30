WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE quality IN ('BAD', 'EST')
      AND site_id <= 8
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY TO_DATE(read_at)), 2) AS read_at_to_date_avg_kwh,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY TO_DATE(read_at))) AS distinct_site_id_in_read_at_to_date,
        SUM(voltage) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_sum_7,
        DENSE_RANK() OVER w_quality_by_read_at AS read_at_dense_rank,
        MIN(voltage) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY TO_DATE(read_at)), 0), 4) AS kwh_share_of_read_at_to_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC, id), 3) AS read_at_percent_rank,
        LEAD(voltage, 1, 0) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id) AS next_voltage,
        SUM(voltage) OVER (PARTITION BY TO_DATE(read_at)) AS read_at_to_date_sum_voltage,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY TO_DATE(read_at)), 2) AS kwh_vs_read_at_to_date_avg
    FROM scoped_readings
    WINDOW w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at NULLS LAST, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_avg_2,
        first_known_read_at,
        read_at_to_date_avg_kwh,
        distinct_site_id_in_read_at_to_date,
        voltage_sum_7,
        min_voltage_so_far,
        kwh_share_of_read_at_to_date,
        read_at_percent_rank,
        next_voltage,
        read_at_to_date_sum_voltage,
        kwh_vs_read_at_to_date_avg
    FROM ranked
    WHERE read_at_dense_rank <= 3
      AND kwh BETWEEN 43.1 AND 117.4
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(read_at_to_date_avg_kwh) OVER (PARTITION BY TO_DATE(read_at)) AS read_at_to_date_min_read_at_to_date_avg_kwh,
        MAX(kwh) OVER (PARTITION BY distinct_site_id_in_read_at_to_date) AS distinct_site_id_in_read_at_to_date_max_kwh,
        LEAD(kwh) OVER (PARTITION BY site_id, quality ORDER BY read_at ASC NULLS LAST, id) AS next_kwh,
        ROUND(kwh_avg_2 / NULLIF(SUM(kwh_avg_2) OVER (PARTITION BY distinct_site_id_in_read_at_to_date), 0), 4) AS kwh_avg_2_share_of_distinct_site_id_in_read_at_t,
        ROUND(kwh_avg_2 - AVG(kwh_avg_2) OVER w_read_at_to_date, 2) AS kwh_avg_2_vs_read_at_to_date_avg,
        LAST_VALUE(read_at_to_date_avg_kwh) IGNORE NULLS OVER (PARTITION BY TO_DATE(first_known_read_at) ORDER BY first_known_read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at_to_date_avg_kwh
    FROM top_rows
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at))
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    first_known_read_at,
    read_at_to_date_avg_kwh,
    distinct_site_id_in_read_at_to_date,
    voltage_sum_7,
    read_at_percent_rank,
    next_voltage,
    kwh_vs_read_at_to_date_avg,
    read_at_to_date_min_read_at_to_date_avg_kwh,
    distinct_site_id_in_read_at_to_date_max_kwh,
    next_kwh,
    kwh_avg_2_share_of_distinct_site_id_in_read_at_t,
    kwh_avg_2_vs_read_at_to_date_avg,
    kwh_avg_2 + voltage AS kwh_avg_2_plus_voltage
FROM with_windows
ORDER BY distinct_site_id_in_read_at_to_date
