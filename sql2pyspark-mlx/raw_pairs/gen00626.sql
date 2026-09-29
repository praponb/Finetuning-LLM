WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        TO_DATE(created_at) AS created_at_date,
        COALESCE(value, 'UNKNOWN') AS value_clean
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 00:00:00'
      AND value IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        CAST(days_since_created_at AS DOUBLE) AS days_since_created_at_str
    FROM filtered
    WHERE value IS NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM cleaned
    INTERSECT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(days_since_created_at_str, 2) AS days_since_created_at_str_rounded
    FROM reconciled
    WHERE created_at >= DATE '2025-03-01'
      AND value IS DISTINCT FROM 'WELCOME5'
)
SELECT
    id,
    value,
    days_since_created_at,
    created_at_date,
    value_clean,
    days_since_created_at_str,
    days_since_created_at_str_rounded
FROM valid
ORDER BY days_since_created_at DESC;
