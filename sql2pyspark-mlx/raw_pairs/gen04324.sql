WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        HOUR(created_at) AS created_at_hour,
        INITCAP(TRIM(attr_key)) AS attr_key_title
    FROM exploded
    WHERE attr_key IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(created_at_hour) OVER (PARTITION BY customer_id) AS customer_id_max_created_at_hour,
        MAX(created_at_hour) OVER w_part AS created_at_hour_max_2h,
        DENSE_RANK() OVER (PARTITION BY attr_key, customer_id ORDER BY created_at_hour NULLS LAST) AS created_at_hour_dense_rank
    FROM cleaned
    WINDOW w_part AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM with_windows
    EXCEPT
    SELECT customer_id, id
    FROM carts
    WHERE TO_DATE(created_at) >= '2024-09-15'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT(*) AS event_count,
    MIN(customer_id_max_created_at_hour) AS min_customer_id_max_created_at_hour,
    COUNT(*) FILTER (WHERE attr_key = 'device') AS device_rows
FROM verified
GROUP BY DATE_TRUNC('DAY', created_at);
