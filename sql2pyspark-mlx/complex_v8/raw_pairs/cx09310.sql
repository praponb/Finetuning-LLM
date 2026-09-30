WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY read_at ASC, id) AS read_at_seq,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY TO_DATE(read_at), quality)) AS distinct_site_id_in_read_at_to_date_quality,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY TO_DATE(read_at), quality), 2) AS kwh_vs_read_at_to_date_quality_avg,
        ROUND(AVG(voltage) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        MAX(read_at_hour) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_read_at_hour_so_far,
        ROUND(voltage - AVG(voltage) OVER w_read_at_to_date_quality, 2) AS voltage_vs_read_at_to_date_quality_avg,
        DENSE_RANK() OVER (ORDER BY read_at DESC NULLS FIRST, id) AS read_at_dense_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY TO_DATE(read_at), quality), 2) AS read_at_to_date_quality_avg_kwh,
        SUM(read_at_hour) OVER (PARTITION BY TO_DATE(read_at), quality) AS read_at_to_date_quality_sum_read_at_hour,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        COUNT(*) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_ties, 3) AS read_at_percent_rank,
        SUM(voltage) OVER (PARTITION BY TO_DATE(read_at), quality) AS read_at_to_date_quality_sum_voltage,
        LAST_VALUE(read_at_hour) OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_hour
    FROM valid_readings
    WINDOW w_read_at_to_date_quality AS (PARTITION BY TO_DATE(read_at), quality), w_all_by_read_at_ties AS (ORDER BY read_at DESC NULLS FIRST)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY read_at_seq), 0), 4) AS kwh_share_of_read_at_seq,
        ROUND(max_read_at_hour_so_far / NULLIF(SUM(max_read_at_hour_so_far) OVER (PARTITION BY read_at_seq), 0), 4) AS max_read_at_hour_so_far_share_of_read_at_seq,
        ROUND(AVG(read_at_seq) OVER (PARTITION BY distinct_site_id_in_read_at_to_date_quality, read_at_dense_rank ORDER BY kwh_vs_read_at_to_date_quality_avg NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS read_at_seq_avg_2,
        MIN(last_read_at_hour) OVER (ORDER BY voltage_avg_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_last_read_at_hour_so_far,
        MAX(read_at_percent_rank) OVER (PARTITION BY TO_DATE(first_known_read_at)) AS first_known_read_at_to_date_max_read_at_percent_,
        COUNT(*) OVER (PARTITION BY TO_DATE(first_known_read_at)) AS rows_in_first_known_read_at_to_date,
        MIN(voltage) OVER (PARTITION BY read_at_seq) AS read_at_seq_min_voltage
    FROM with_windows
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality_ref,
    read_at_hour,
    read_at_seq,
    distinct_site_id_in_read_at_to_date_quality,
    kwh_vs_read_at_to_date_quality_avg,
    max_read_at_hour_so_far,
    read_at_dense_rank,
    read_at_to_date_quality_avg_kwh,
    first_known_kwh,
    running_count,
    read_at_percent_rank,
    read_at_to_date_quality_sum_voltage,
    last_read_at_hour,
    max_read_at_hour_so_far_share_of_read_at_seq,
    read_at_seq_avg_2,
    min_last_read_at_hour_so_far,
    first_known_read_at_to_date_max_read_at_percent_,
    rows_in_first_known_read_at_to_date,
    DATE_FORMAT(first_known_read_at, 'yyyy-MM') AS first_known_read_at_label
FROM windowed
WHERE rows_in_first_known_read_at_to_date >= 4
ORDER BY read_at_seq_avg_2
