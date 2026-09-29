WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 07:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY quality_len ASC), 3) AS quality_len_percent_rank,
        NTH_VALUE(kwh, 2) OVER (ORDER BY quality_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        RANK() OVER w_all_by_quality_len AS quality_len_rank,
        DENSE_RANK() OVER (ORDER BY quality_len ASC, id) AS quality_len_dense_rank
    FROM base_readings
    WINDOW w_all_by_quality_len AS (ORDER BY quality_len ASC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        quality_len_rank,
        quality,
        quality_len,
        SUM(quality_len) AS total_quality_len,
        MIN(read_at) AS first_read_at,
        COUNT(*) AS cnt
    FROM windowed
    GROUP BY 1, 2, 3
    HAVING MAX(quality_len_dense_rank) > 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality_len_rank, quality, metric, amount
FROM (
    SELECT quality_len_rank, quality, quality_len, total_quality_len
    FROM stats
) src
UNPIVOT (
    amount FOR metric IN (quality_len, total_quality_len)
) unp
ORDER BY quality_len_rank, quality, metric
