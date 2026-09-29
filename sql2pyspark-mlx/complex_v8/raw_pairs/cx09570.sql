WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage < 241.8
      AND read_at < TIMESTAMP '2025-02-12 14:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        days_since_read_at,
        HOUR(read_at) AS read_at_hour,
        CASE WHEN days_since_read_at >= 595 THEN 'A' WHEN days_since_read_at >= 210 THEN 'B' ELSE 'C' END AS days_since_read_at_tier,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM recent
    WHERE voltage BETWEEN 219.6 AND 232.0
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality NOT IN ('OK', 'EST')
              AND m.read_at >= TIMESTAMP '2025-02-06 14:00:00'
        )
      AND s.days_since_read_at_tier = 'A'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        read_at_hour,
        MIN(read_at) AS first_read_at,
        MAX(read_at) AS last_read_at,
        PERCENTILE_APPROX(voltage_bucket, 0.5) AS median_voltage_bucket
    FROM with_activity
    GROUP BY DATE_TRUNC('MONTH', read_at), read_at_hour
    HAVING MAX(read_at_hour) > 8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        read_at_month,
        read_at_hour,
        first_read_at,
        last_read_at,
        DENSE_RANK() OVER w_first_read_at_month_read_at AS last_read_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY read_at_hour, TO_DATE(read_at_month) ORDER BY first_read_at DESC) AS first_read_at_dense_rank,
        median_voltage_bucket - LAG(median_voltage_bucket) OVER w_read_at_hour_read_at_month_t_2 AS median_voltage_bucket_change,
        ROUND(PERCENT_RANK() OVER w_first_read_at_month_read_at_2, 3) AS last_read_at_percent_rank
    FROM summary
    WINDOW w_first_read_at_month_read_at AS (PARTITION BY DATE_TRUNC('MONTH', first_read_at), read_at_hour ORDER BY last_read_at DESC), w_read_at_hour_read_at_month_t_2 AS (PARTITION BY read_at_hour, TO_DATE(read_at_month) ORDER BY first_read_at DESC, read_at_month, read_at_hour), w_first_read_at_month_read_at_2 AS (PARTITION BY DATE_TRUNC('MONTH', first_read_at), read_at_hour ORDER BY last_read_at DESC, read_at_month, read_at_hour)
)
-- Final projection
SELECT
    read_at_month,
    read_at_hour,
    last_read_at,
    last_read_at_dense_rank,
    first_read_at_dense_rank,
    median_voltage_bucket_change,
    COALESCE(last_read_at_dense_rank, 0) AS last_read_at_dense_rank_filled
FROM with_trends
ORDER BY last_read_at DESC NULLS FIRST, read_at_month, read_at_hour
LIMIT 20
