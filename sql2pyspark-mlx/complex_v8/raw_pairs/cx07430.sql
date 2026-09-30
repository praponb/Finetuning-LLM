WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE kwh >= 20.4
      AND quality IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        ROW_NUMBER() OVER w_site_id_by_voltage_plus_kwh AS seq_no,
        SUM(voltage_plus_kwh) OVER (PARTITION BY site_id ORDER BY voltage_plus_kwh DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_plus_kwh_sum_1_prior,
        COUNT(*) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS rows_in_site_id_read_at_to_date,
        voltage_plus_kwh - LAG(voltage_plus_kwh) OVER w_all_by_kwh AS voltage_plus_kwh_change,
        SUM(kwh) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS site_id_read_at_to_date_sum_kwh,
        LEAD(read_at, 2) OVER w_read_at_month_site_id_by_vol AS next2_read_at,
        ROUND(voltage - AVG(voltage) OVER w_site_id_read_at_to_date, 2) AS voltage_vs_site_id_read_at_to_date_avg,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY voltage_plus_kwh DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at)), 0), 4) AS voltage_share_of_site_id_read_at_to_date,
        MIN(voltage_plus_kwh) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS site_id_read_at_to_date_min_voltage_plus_kwh,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id ORDER BY voltage_plus_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        SUM(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_sum_7
    FROM filtered_readings
    WINDOW w_site_id_by_voltage_plus_kwh AS (PARTITION BY site_id ORDER BY voltage_plus_kwh DESC, id), w_all_by_kwh AS (ORDER BY kwh DESC, id), w_read_at_month_site_id_by_vol AS (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY voltage_plus_kwh, id), w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at))
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE seq_no = 1
      AND kwh_avg_2_prior > 42.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        rows_in_site_id_read_at_to_date,
        site_id_read_at_to_date_sum_kwh,
        next2_read_at,
        voltage_vs_site_id_read_at_to_date_avg,
        voltage_share_of_site_id_read_at_to_date,
        kwh_sum_7,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY seq_no ORDER BY site_id_read_at_to_date_min_voltage_plus_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        ROUND(kwh_avg_2_prior - AVG(kwh_avg_2_prior) OVER (PARTITION BY DATE_TRUNC('MONTH', second_read_at), seq_no), 2) AS kwh_avg_2_prior_vs_second_read_at_month_seq_no_a,
        SIZE(COLLECT_SET(site_id) OVER w_second_read_at_month_seq_no) AS distinct_site_id_in_second_read_at_month_seq_no,
        SUM(voltage_plus_kwh_sum_1_prior) OVER (PARTITION BY DATE_TRUNC('MONTH', second_read_at), seq_no) AS second_read_at_month_seq_no_sum_voltage_plus_kwh,
        ROUND(AVG(voltage_plus_kwh_change) OVER (PARTITION BY seq_no ORDER BY seq_no ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_plus_kwh_change_avg_2
    FROM top_per_group
    WINDOW w_second_read_at_month_seq_no AS (PARTITION BY DATE_TRUNC('MONTH', second_read_at), seq_no)
)
-- Final projection
SELECT
    id,
    site_id,
    next2_read_at,
    voltage_vs_site_id_read_at_to_date_avg,
    voltage_share_of_site_id_read_at_to_date,
    kwh_sum_7,
    last_known_seq_no,
    kwh_avg_2_prior_vs_second_read_at_month_seq_no_a,
    distinct_site_id_in_second_read_at_month_seq_no,
    second_read_at_month_seq_no_sum_voltage_plus_kwh,
    voltage_plus_kwh_change_avg_2,
    ABS(kwh_sum_7 - 198.0) AS kwh_sum_7_distance
FROM ranked
WHERE voltage_share_of_site_id_read_at_to_date > 0.2
