WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, MONTH(created_at) AS created_at_month
    FROM exploded
    WHERE created_at >= TIMESTAMP '2025-03-01 14:00:00'
      AND key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(key) AS key_lower,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM valid
    WHERE customer_id = 17
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        key,
        attr_value,
        created_at_month,
        key_lower,
        days_since_created_at,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at_2,
        COALESCE(key, 'NONE') AS key_filled
    FROM filtered
    WHERE created_at > DATE '2025-03-02'
      AND key IS NOT NULL
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(days_since_created_at_2 * 1.07, 2) AS days_since_created_at_2_scaled,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM base
    WHERE created_at > TIMESTAMP '2025-03-01 12:00:00'
      AND key IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    ROUND(AVG(days_since_created_at), 2) AS avg_days_since_created_at,
    COUNT(DISTINCT key) AS distinct_keys,
    SUM(created_at_month) FILTER (WHERE key = 'weight_kg') AS weight_kg_total_created_at_month,
    GROUPING_ID() AS gid
FROM valid_carts
GROUP BY ROLLUP (DATE_TRUNC('HOUR', created_at))
HAVING avg_days_since_created_at < 0
