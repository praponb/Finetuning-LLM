WITH flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    SELECT
        id,
        reading_ts,
        SUM(temp_c) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_id, id) seq_no,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_humidity,
        AVG(temp_c) OVER (PARTITION BY DATE_TRUNC('month', reading_ts), device_code ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) temp_c_avg_6_prior,
        CORR(status_bits, temp_c) OVER (PARTITION BY DATE_TRUNC('YEAR', reading_ts)) corr_status_bits_temp_c
    FROM sessionized
),
with_windows AS (
    SELECT
        id,
        reading_ts,
        running_temp_c,
        seq_no,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('month', reading_ts) ORDER BY temp_c_avg_6_prior NULLS LAST) temp_c_avg_6_prior_percent_rank,
        FIRST_VALUE(corr_status_bits_temp_c, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_corr_status_bits_temp_c,
        SUM(corr_status_bits_temp_c) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_corr_status_bits_temp_c,
        AVG(temp_c_avg_6_prior) OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY last_known_humidity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) temp_c_avg_6_prior_avg_5_prior
    FROM enriched
),
valid AS (
    SELECT
        id,
        reading_ts,
        running_temp_c,
        seq_no,
        temp_c_avg_6_prior_percent_rank,
        first_known_corr_status_bits_temp_c,
        running_corr_status_bits_temp_c,
        temp_c_avg_6_prior_avg_5_prior,
        ROUND(temp_c_avg_6_prior_percent_rank, 1) temp_c_avg_6_prior_percent_rank_rounded
    FROM with_windows
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
),
metrics AS (
    SELECT
        id,
        running_temp_c,
        temp_c_avg_6_prior_percent_rank,
        first_known_corr_status_bits_temp_c,
        temp_c_avg_6_prior_percent_rank_rounded,
        ROW_NUMBER() OVER (PARTITION BY YEAR(reading_ts) ORDER BY reading_ts DESC, id) seq_no_2,
        FIRST_VALUE(temp_c_avg_6_prior_percent_rank, TRUE) OVER (ORDER BY temp_c_avg_6_prior_percent_rank_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_temp_c_avg_6_prior_percent_rank,
        COUNT(seq_no) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) seq_no_count_15m
    FROM valid
)
SELECT
    id,
    first_known_corr_status_bits_temp_c,
    first_known_temp_c_avg_6_prior_percent_rank,
    seq_no_count_15m
FROM metrics
ORDER BY first_known_temp_c_avg_6_prior_percent_rank NULLS FIRST;
