WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        paid_at,
        CEIL(amount) AS amount_whole
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-11-07 04:00:00')
      AND order_id IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(method) AS method_length,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM cleaned
    WHERE paid_at >= TIMESTAMP '2023-12-24 17:00:00'
),
matched_keys AS (
    -- Set difference on keys
    SELECT method
    FROM valid
    EXCEPT ALL
    SELECT method
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 11 AND 18
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.method = k.method
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        paid_at,
        method_filled,
        FLOOR(amount_whole) AS amount_whole_whole
    FROM verified
    WHERE method = 'cash'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        paid_at,
        method_filled,
        amount_whole_whole,
        SUM(amount_whole_whole) OVER (PARTITION BY method) AS method_sum_amount_whole_whole,
        AVG(amount_whole_whole) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_whole_whole_avg_7d,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount_whole_whole DESC NULLS LAST) AS amount_whole_whole_dense_rank
    FROM cleaned_payments
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        method_filled,
        amount_whole_whole,
        method_sum_amount_whole_whole,
        amount_whole_whole_avg_7d,
        amount_whole_whole_dense_rank,
        DATE_TRUNC('month', paid_at) AS paid_at_period
    FROM scored
    WHERE TO_DATE(paid_at) < '2023-03-09'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', paid_at_period) AS quarter_start,
        DATE_TRUNC('YEAR', paid_at_period) AS year_start,
        (UNIX_TIMESTAMP(MAX(paid_at_period)) - UNIX_TIMESTAMP(MIN(paid_at_period))) / 60 AS span_minutes,
        (UNIX_TIMESTAMP(MAX(paid_at_period)) - UNIX_TIMESTAMP(MIN(paid_at_period))) / 60 AS duration_min,
        MIN(paid_at_period) AS first_paid_at_period
    FROM filtered
    GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', paid_at_period), DATE_TRUNC('YEAR', paid_at_period)), (DATE_TRUNC('QUARTER', paid_at_period)), ())
)
SELECT
    quarter_start,
    year_start,
    span_minutes,
    duration_min,
    first_paid_at_period
FROM summary
ORDER BY first_paid_at_period DESC NULLS LAST
