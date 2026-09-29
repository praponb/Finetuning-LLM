WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(key, 'unassigned') AS key_filled,
        YEAR(created_at) AS created_at_year,
        COALESCE(key, 'NONE') AS resolved_key
    FROM flattened
    WHERE customer_id IS NOT NULL
      AND key IN ('utm_source', 'device')
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.signup_date,
        p.credit_limit
    FROM filtered s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, d.country
    FROM enriched e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        key,
        value,
        key_filled,
        created_at_year,
        credit_limit,
        country,
        SUM(created_at_year) OVER (PARTITION BY customer_id, country) AS customer_id_sum_created_at_year,
        AVG(credit_limit) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS credit_limit_avg_30m,
        COUNT(created_at_year) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_year_count_2h
    FROM joined
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM metrics
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
)
SELECT
    customer_id,
    value,
    key_filled,
    created_at_year,
    credit_limit,
    country,
    customer_id_sum_created_at_year,
    credit_limit_avg_30m
FROM verified
ORDER BY customer_id_sum_created_at_year DESC NULLS FIRST
