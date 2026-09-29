WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM exploded
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE customer_id = 19
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(value) AS clean_value
    FROM kept_rows
    WHERE created_at > TO_TIMESTAMP('2025-03-02 09:00:00')
      AND value IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(value)) AS value_title,
        DATE_TRUNC('WEEK', created_at) AS created_at_period,
        COALESCE(value, 'UNKNOWN') AS value_filled
    FROM cleaned
    WHERE HOUR(created_at) BETWEEN 9 AND 15
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', created_at_period) AS activity_month,
    value,
    MIN(created_at) AS started_at,
    SIZE(COLLECT_SET(value)) AS num_values,
    (UNIX_TIMESTAMP(MAX(created_at_period)) - UNIX_TIMESTAMP(MIN(created_at_period))) / 60 AS span_minutes,
    MAX(created_at_period) AS last_created_at_period
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', created_at_period), value), (DATE_TRUNC('MONTH', created_at_period)))
ORDER BY activity_month;
