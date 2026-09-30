WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE (voltage <= 234.5 OR quality IN ('EST', 'OK'))
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY TO_DATE(read_at)), 2) AS voltage_vs_read_at_to_date_avg,
        DENSE_RANK() OVER w_site_id_quality_by_voltage_t AS voltage_dense_rank,
        voltage - LAG(voltage) OVER w_quality_site_id_by_voltage AS voltage_change,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage ASC, id) AS voltage_dense_rank_2,
        LAST_VALUE(voltage) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        LEAD(read_at) OVER w_quality_site_id_by_voltage AS next_read_at,
        DENSE_RANK() OVER (ORDER BY read_at DESC, id) AS read_at_dense_rank,
        NTILE(5) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY read_at DESC, id) AS read_at_quintile,
        DENSE_RANK() OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY read_at DESC, id) AS read_at_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY read_at ASC) AS read_at_dense_rank_3,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage ASC, id) AS seq_no,
        SUM(voltage) OVER (PARTITION BY site_id, quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage), 3) AS voltage_percent_rank,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage ASC) AS voltage_dense_rank_3
    FROM base
    WINDOW w_site_id_quality_by_voltage_t AS (PARTITION BY site_id, quality ORDER BY voltage), w_quality_site_id_by_voltage AS (PARTITION BY quality, site_id ORDER BY voltage ASC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(read_at_dense_rank, 0) AS read_at_dense_rank_filled
    FROM sequenced
    WHERE read_at >= TIMESTAMP '2025-02-06 13:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        voltage,
        quality,
        voltage_dense_rank,
        voltage_dense_rank_2,
        last_voltage,
        next_read_at,
        read_at_dense_rank,
        read_at_dense_rank_2,
        read_at_dense_rank_3,
        seq_no,
        voltage_percent_rank,
        ROUND(read_at_quintile / NULLIF(SUM(read_at_quintile) OVER (PARTITION BY site_id, read_at_dense_rank_2), 0), 4) AS read_at_quintile_share_of_site_id_read_at_dense_,
        COUNT(*) OVER w_read_at_dense_rank_2_read_at AS rows_in_read_at_dense_rank_2_read_at_dense_rank_,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY next_read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        MAX(read_at_dense_rank_3) OVER w_read_at_dense_rank_2_read_at AS read_at_dense_rank_2_read_at_dense_rank_3_max_re,
        LAG(seq_no) OVER (PARTITION BY read_at_quintile ORDER BY running_voltage, id) AS prev_seq_no,
        FIRST_VALUE(read_at_dense_rank) IGNORE NULLS OVER (PARTITION BY voltage_dense_rank_2 ORDER BY read_at_dense_rank_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_dense_rank,
        SUM(voltage_percent_rank) OVER (PARTITION BY read_at_quintile ORDER BY running_voltage, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_percent_rank_sum_3,
        LEAD(last_voltage, 1, 0) OVER (PARTITION BY read_at_dense_rank_filled ORDER BY voltage_dense_rank, id) AS next_last_voltage
    FROM labeled
    WINDOW w_read_at_dense_rank_2_read_at AS (PARTITION BY read_at_dense_rank_2, read_at_dense_rank_3)
),
totals AS (
    -- Group-level totals
    SELECT
        read_at_dense_rank_2,
        COUNT(*) AS cnt,
        ROUND(SUM(last_voltage), 2) AS total_last_voltage,
        COUNT(DISTINCT quality) AS distinct_qualitys
    FROM windowed
    GROUP BY read_at_dense_rank_2
)
-- Final projection
SELECT
    read_at_dense_rank_2,
    cnt,
    LEAST(read_at_dense_rank_2, total_last_voltage) AS least_read_at_dense_rank_2_total_last_voltage,
    ROUND(total_last_voltage * 0.9, 2) AS total_last_voltage_adj
FROM totals
WHERE distinct_qualitys <= 6
ORDER BY total_last_voltage_adj NULLS LAST
