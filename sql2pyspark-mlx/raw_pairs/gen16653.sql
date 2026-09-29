WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 01:00:00'
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, c.email
    FROM cleaned s
    LEFT JOIN customers c
        ON s.customer_id = c.id
    WHERE c.name IS NOT NULL
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.value,
        src.email,
        d.credit_limit,
        d.segment,
        d.name
    FROM joined src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        email,
        name,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id, value), 4) AS credit_limit_share,
        STDDEV_SAMP(credit_limit) OVER w_part AS segment_stddev_samp_credit_limit,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank,
        COUNT(credit_limit) OVER (PARTITION BY value) AS value_count_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY value), 4) AS credit_limit_share_2
    FROM joined_carts
    WINDOW w_part AS (PARTITION BY segment)
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        name,
        value_count_credit_limit,
        DENSE_RANK() OVER (ORDER BY credit_limit_share NULLS LAST) AS credit_limit_share_dense_rank
    FROM with_windows
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(name) AS name_length,
        SUBSTRING(name, 1, 4) AS name_prefix
    FROM with_windows_carts
    WHERE value_count_credit_limit > 15
)
-- Window metrics over several different partitions
SELECT
    email,
    name,
    credit_limit_share_dense_rank,
    name_length,
    CUME_DIST() OVER w_recent AS name_length_cume_dist,
    RANK() OVER w_hist AS value_count_credit_limit_rank,
    RANK() OVER (ORDER BY name_length DESC NULLS LAST) AS name_length_rank
FROM cleaned_carts
WINDOW w_recent AS (ORDER BY name_length NULLS FIRST), w_hist AS (ORDER BY value_count_credit_limit DESC NULLS FIRST)
