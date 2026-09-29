WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 215.6
      AND TO_DATE(read_at) < DATE '2025-02-06'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(kwh, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at DESC, id) AS prev3_kwh,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at DESC, id) AS read_at_seq,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS rows_in_read_at_month_quality,
        MAX(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS read_at_month_quality_max_kwh,
        SUM(kwh) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', read_at)) AS quality_read_at_month_sum_kwh,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        RANK() OVER (PARTITION BY site_id ORDER BY kwh) AS kwh_rank,
        ROUND(AVG(voltage) OVER w_read_at_month_quality, 2) AS read_at_month_quality_avg_voltage,
        LEAD(voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh, id) AS next_voltage,
        LEAD(voltage, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at DESC, id) AS next_voltage_2,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_2,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY kwh), 3) AS kwh_cume_dist,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_3,
        LEAD(kwh, 2) OVER w_quality_site_id_by_kwh_2 AS next2_kwh
    FROM scoped
    WINDOW w_read_at_month_quality AS (PARTITION BY DATE_TRUNC('MONTH', read_at), quality), w_quality_site_id_by_kwh_2 AS (PARTITION BY quality, site_id ORDER BY kwh DESC, id)
),
annotated AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(quality)) AS quality_len
    FROM metrics
    WHERE kwh < 85.7
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY read_at_seq, quality ORDER BY second_voltage_2, id) AS seq_no,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY read_at_seq)) AS distinct_site_id_in_read_at_seq,
        ROUND(AVG(next_voltage) OVER (PARTITION BY read_at_seq), 2) AS read_at_seq_avg_next_voltage,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY read_at_seq)) AS distinct_quality_in_read_at_seq,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY read_at_seq, quality ORDER BY second_voltage_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY read_at_seq), 0), 4) AS voltage_share_of_read_at_seq
    FROM annotated
),
rollup_base AS (
    -- Group-level totals
    SELECT
        seq_no,
        read_at_seq,
        SUM(next_voltage) FILTER (WHERE quality IN ('BAD', 'EST')) AS next_voltage_quality_in_bad_est,
        MIN(read_at) AS first_read_at,
        ROUND(AVG(next_voltage), 2) AS avg_next_voltage
    FROM sequenced
    GROUP BY seq_no, read_at_seq
    HAVING AVG(quality_read_at_month_sum_kwh) > 420.0
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(first_read_at) OVER (PARTITION BY read_at_seq ORDER BY avg_next_voltage ASC, seq_no, read_at_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_read_at,
        COUNT(*) OVER (PARTITION BY MONTH(first_read_at), read_at_seq ORDER BY avg_next_voltage DESC, seq_no, read_at_seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER w_read_at_seq_by_avg_next_volt_2, 3) AS avg_next_voltage_cume_dist,
        LAG(first_read_at) OVER (PARTITION BY MONTH(first_read_at), read_at_seq ORDER BY avg_next_voltage DESC, seq_no, read_at_seq) AS prev_first_read_at
    FROM rollup_base
    WINDOW w_read_at_seq_by_avg_next_volt_2 AS (PARTITION BY read_at_seq ORDER BY avg_next_voltage ASC, seq_no, read_at_seq)
)
SELECT
    seq_no,
    read_at_seq,
    next_voltage_quality_in_bad_est,
    first_read_at,
    avg_next_voltage,
    last_first_read_at,
    running_count,
    avg_next_voltage_cume_dist,
    running_count + read_at_seq AS running_count_plus_read_at_seq
FROM windowed
ORDER BY last_first_read_at NULLS LAST
