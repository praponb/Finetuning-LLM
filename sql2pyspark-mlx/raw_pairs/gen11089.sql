WITH base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(channel)) AS channel_title
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'partner'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM orders
    WHERE store_id IN (7, 15)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.store_id,
        src.order_date,
        src.status,
        src.channel,
        d.signup_date,
        d.age
    FROM verified src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
    WHERE d.signup_date > '2025-09-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(age AS DOUBLE) AS age_str,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM with_dims
    WHERE channel IS NOT NULL
      AND channel IS DISTINCT FROM 'partner'
),
base_orders AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_label,
        LAST_DAY(signup_date) AS signup_date_month_end,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM filtered
    WHERE age_str >= 61
      AND customer_id IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    s.city,
    e.channel,
    COUNT(*) AS cnt,
    MAX(e.age_str) AS max_age_str
FROM base_orders e
INNER JOIN stores s ON e.store_id = s.id
GROUP BY GROUPING SETS ((s.city, e.channel), (s.city))
