WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE voltage <= 233.6
      AND site_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_minus_voltage,
        SUM(kwh) OVER (PARTITION BY site_id, quality) AS site_id_quality_sum_kwh,
        LEAD(read_at, 3) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC NULLS FIRST, id) AS next3_read_at,
        LAG(kwh_minus_voltage, 3) OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh DESC, id) AS prev3_kwh_minus_voltage,
        kwh - LAG(kwh) OVER (PARTITION BY site_id, quality ORDER BY read_at DESC NULLS FIRST, id) AS kwh_change,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY kwh_minus_voltage DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS rows_in_read_at_month_quality,
        MAX(kwh_minus_voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS read_at_month_quality_max_kwh_minus_voltage,
        ROUND(kwh - AVG(kwh) OVER w_site_id_quality, 2) AS kwh_vs_site_id_quality_avg,
        FIRST_VALUE(kwh_minus_voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_minus_voltage,
        ROUND(voltage - AVG(voltage) OVER w_site_id_quality, 2) AS voltage_vs_site_id_quality_avg,
        MAX(kwh_minus_voltage) OVER (PARTITION BY site_id ORDER BY kwh_minus_voltage DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_minus_voltage_max_3,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality)) AS distinct_site_id_in_read_at_month_quality,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh_minus_voltage DESC NULLS FIRST, id) AS kwh_minus_voltage_seq,
        ROW_NUMBER() OVER w_read_at_to_date_by_kwh AS rn,
        LEAD(read_at) OVER w_quality_site_id_by_kwh AS next_read_at
    FROM prepared
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality), w_read_at_to_date_by_kwh AS (PARTITION BY TO_DATE(read_at) ORDER BY kwh DESC, id), w_quality_site_id_by_kwh AS (PARTITION BY quality, site_id ORDER BY kwh DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        CASE WHEN voltage >= 232.4 THEN 'large' WHEN voltage >= 224.8 THEN 'medium' ELSE 'small' END AS voltage_tier,
        ROUND(100 * COUNT(CASE WHEN quality = 'EST' THEN 1 END) / COUNT(*), 1) AS pct_quality_est,
        MIN(voltage) AS min_voltage,
        ROUND(SUM(kwh_minus_voltage_seq) / NULLIF(SUM(kwh_avg_3), 0), 3) AS kwh_minus_voltage_seq_to_kwh_avg_3_ratio,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM sequenced
    GROUP BY DATE_TRUNC('MONTH', read_at), CASE WHEN voltage >= 232.4 THEN 'large' WHEN voltage >= 224.8 THEN 'medium' ELSE 'small' END
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_row_count_voltage_tier_by_mi AS min_voltage_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_month), row_count) AS rows_in_read_at_month_month_row_count,
        ROW_NUMBER() OVER (PARTITION BY row_count, voltage_tier ORDER BY min_voltage, read_at_month, voltage_tier) AS min_voltage_seq
    FROM grouped
    WINDOW w_row_count_voltage_tier_by_mi AS (PARTITION BY row_count, voltage_tier ORDER BY min_voltage)
)
-- Final projection
SELECT
    read_at_month,
    pct_quality_est,
    min_voltage,
    kwh_minus_voltage_seq_to_kwh_avg_3_ratio,
    row_count,
    min_voltage_rank,
    rows_in_read_at_month_month_row_count,
    min_voltage_seq,
    TO_DATE(read_at_month) AS read_at_month_date
FROM metrics
WHERE min_voltage_seq <= 9
ORDER BY read_at_month_date DESC
