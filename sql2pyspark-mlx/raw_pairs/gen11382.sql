WITH joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.plan,
        e.start_date,
        e.end_date,
        e.status,
        c.email,
        c.credit_limit,
        c.name
    FROM subscriptions e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status,
        email,
        credit_limit,
        LAST_DAY(end_date) AS end_date_month_end
    FROM joined
    WHERE end_date BETWEEN '2023-06-01' AND '2024-08-01'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM base
    EXCEPT DISTINCT
    SELECT customer_id, status
    FROM subscriptions
    WHERE plan = 'standard'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(end_date), status ORDER BY credit_limit NULLS LAST) AS credit_limit_dense_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY plan), 4) AS credit_limit_share
    FROM verified
    WHERE customer_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.city,
        d.name
    FROM scored s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        credit_limit_share,
        city,
        PERCENT_RANK() OVER (PARTITION BY customer_id, status ORDER BY credit_limit_dense_rank) AS credit_limit_dense_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank_2
    FROM with_dims
),
joined_subscriptions AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.credit_limit_share,
        s.city,
        s.credit_limit_dense_rank_percent_rank,
        b.signup_date,
        b.name
    FROM enriched s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit_dense_rank_percent_rank >= 0.73 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_dense_rank_percent_rank_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN city = 'Bangkok' THEN credit_limit_dense_rank_percent_rank ELSE 0 END) AS bangkok_credit_limit_dense_rank_percent_rank
FROM joined_subscriptions
GROUP BY 1
HAVING SUM(CASE WHEN city = 'Bangkok' THEN credit_limit_dense_rank_percent_rank ELSE 0 END) >= 0
