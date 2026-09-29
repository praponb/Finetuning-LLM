WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(TRIM(quality)) AS quality_clean,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 6 AND 9
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(quality_len) OVER (PARTITION BY quality_len ORDER BY read_at, id) AS prev_quality_len,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY quality_len ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(voltage - AVG(voltage) OVER w_quality_len, 2) AS voltage_vs_quality_len_avg,
        SUM(quality_len) OVER (ORDER BY quality_len ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quality_len_sum_7,
        SUM(quality_len) OVER (PARTITION BY quality_len) AS quality_len_sum_quality_len,
        COUNT(*) OVER (PARTITION BY quality_len) AS rows_in_quality_len
    FROM base
    WINDOW w_quality_len AS (PARTITION BY quality_len)
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 232.4
        )
      AND s.kwh BETWEEN 42.1 AND 81.3
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(prev_quality_len - AVG(prev_quality_len) OVER (PARTITION BY site_id, prev_quality_len), 2) AS prev_quality_len_vs_site_id_prev_quality_len_avg,
        ROUND(AVG(prev_quality_len) OVER (PARTITION BY site_id, prev_quality_len), 2) AS site_id_prev_quality_len_avg_prev_quality_len,
        FIRST_VALUE(quality_len) IGNORE NULLS OVER (PARTITION BY site_id, prev_quality_len ORDER BY rows_in_quality_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quality_len,
        FIRST_VALUE(quality_len) OVER (PARTITION BY site_id, prev_quality_len ORDER BY rows_in_quality_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_quality_len
    FROM qualified
),
with_windows_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id, first_known_quality_len) AS rows_in_site_id_first_known_quality_len,
        LEAD(quality_len_sum_quality_len) OVER (ORDER BY second_read_at ASC, id) AS next_quality_len_sum_quality_len,
        ROUND(PERCENT_RANK() OVER (ORDER BY second_read_at ASC), 3) AS second_read_at_percent_rank,
        ROUND(quality_len_sum_7 / NULLIF(SUM(quality_len_sum_7) OVER (PARTITION BY site_id, first_known_quality_len), 0), 4) AS quality_len_sum_7_share_of_site_id_first_known_q,
        first_known_quality_len - LAG(first_known_quality_len) OVER (PARTITION BY quality_len, prev_quality_len ORDER BY quality_len, id) AS first_known_quality_len_change,
        DENSE_RANK() OVER (PARTITION BY quality_len, prev_quality_len ORDER BY quality_len, id) AS quality_len_dense_rank
    FROM with_windows_2
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM with_windows_3 s
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_clean,
    quality_len,
    prev_quality_len,
    voltage_vs_quality_len_avg,
    quality_len_sum_7,
    quality_len_sum_quality_len,
    rows_in_quality_len,
    site_id_prev_quality_len_avg_prev_quality_len,
    rows_in_site_id_first_known_quality_len,
    next_quality_len_sum_quality_len,
    quality_len_dense_rank,
    DATEDIFF(DATE '2025-06-30', first_reading_read_at) AS days_since_first_reading_read_at
FROM profiled
WHERE prev_quality_len_vs_site_id_prev_quality_len_avg < 4.2
ORDER BY site_id_prev_quality_len_avg_prev_quality_len, voltage_vs_quality_len_avg DESC
