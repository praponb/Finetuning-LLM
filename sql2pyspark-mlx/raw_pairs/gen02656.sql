WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        firmware,
        ROUND(status_bits * 1.1, 2) AS status_bits_adjusted,
        CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE humidity < 51
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(reading_ts_day) ORDER BY temp_c DESC, id) AS row_seq,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY status_bits_adjusted, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS temp_c_avg_3_prior,
        PERCENT_RANK() OVER (PARTITION BY YEAR(reading_ts_day) ORDER BY status_bits_adjusted DESC) AS status_bits_adjusted_percent_rank,
        LAG(status_bits_adjusted, 1) OVER (PARTITION BY device_code ORDER BY status_bits_adjusted, id) AS prev_status_bits_adjusted
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        humidity,
        status_bits_adjusted,
        status_bits_adjusted_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', reading_ts_day) ORDER BY temp_c_avg_3_prior DESC, id) AS row_seq_2,
        CUME_DIST() OVER (ORDER BY temp_c NULLS LAST) AS temp_c_cume_dist
    FROM scored
),
enriched_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(row_seq_2) OVER (ORDER BY status_bits_adjusted, id) AS prev_row_seq_2,
        LEAD(row_seq_2) OVER (ORDER BY status_bits_adjusted, id) AS next_row_seq_2,
        LEAD(status_bits_adjusted_percent_rank) OVER (ORDER BY status_bits_adjusted, id) AS next_status_bits_adjusted_percent_rank,
        ROW_NUMBER() OVER (ORDER BY status_bits_adjusted, id) AS event_seq
    FROM enriched
)
SELECT
    id,
    temp_c,
    humidity,
    temp_c_cume_dist,
    prev_row_seq_2,
    event_seq
FROM enriched_device_readings;
