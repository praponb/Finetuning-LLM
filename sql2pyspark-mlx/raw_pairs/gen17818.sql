WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.value,
        c.credit_limit
    FROM flattened a
    INNER JOIN customers c
        ON a.customer_id = c.id
    WHERE c.credit_limit >= 9000
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY credit_limit NULLS FIRST) AS credit_limit_cume_dist,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_dense_rank,
        DENSE_RANK() OVER (PARTITION BY value ORDER BY credit_limit NULLS FIRST) AS credit_limit_dense_rank_2,
        RANK() OVER (PARTITION BY value ORDER BY credit_limit) AS credit_limit_rank
    FROM joined
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY credit_limit_cume_dist NULLS FIRST) AS credit_limit_cume_dist_cume_dist,
        SUM(credit_limit) OVER (PARTITION BY value) AS value_sum_credit_limit
    FROM scored
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.value,
        s.credit_limit,
        s.credit_limit_rank,
        s.value_sum_credit_limit,
        p.email
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM with_dims
    EXCEPT DISTINCT
    SELECT customer_id
    FROM carts
    WHERE customer_id IN (11, 22, 28)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
)
SELECT
    customer_id,
    value,
    email
FROM verified
ORDER BY customer_id DESC;
