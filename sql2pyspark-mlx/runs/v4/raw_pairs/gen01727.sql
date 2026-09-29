WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM lines
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 6 AND 14
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN element = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5
    FROM kept_rows
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 09:00:00')
      AND element IN ('VIP', 'WELCOME5', 'SPRING10')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.country,
        p.email
    FROM cleaned s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        created_at,
        element,
        is_welcome5,
        country,
        email,
        AVG(is_welcome5) OVER (PARTITION BY customer_id) AS customer_id_avg_is_welcome5,
        DENSE_RANK() OVER (PARTITION BY element ORDER BY is_welcome5 NULLS LAST) AS is_welcome5_dense_rank,
        AVG(is_welcome5) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS is_welcome5_avg_15m,
        AVG(is_welcome5) OVER (PARTITION BY country ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS is_welcome5_avg_30m,
        MAX(is_welcome5) OVER (PARTITION BY element) AS element_max_is_welcome5
    FROM with_dims
)
SELECT
    customer_id,
    is_welcome5,
    country,
    email,
    is_welcome5_avg_15m,
    element_max_is_welcome5
FROM with_windows
ORDER BY country DESC NULLS LAST, is_welcome5
