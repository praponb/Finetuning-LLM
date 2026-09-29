WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC, id ASC) AS dedup_rank
    FROM subscriptions
    QUALIFY dedup_rank = 1
),
latest_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, b.signup_date
    FROM latest_subscriptions e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        signup_date
    FROM joined
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC NULLS FIRST) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM ranked
    EXCEPT
    SELECT customer_id, id
    FROM subscriptions
    WHERE monthly_fee > 120
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined_subscriptions AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        p.country,
        p.credit_limit,
        p.city
    FROM kept_rows s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        country,
        CASE WHEN credit_limit >= 17700 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
        SUM(credit_limit) FILTER (WHERE city = 'New York') AS new_york_total_credit_limit,
        COUNT(*) AS row_count,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        GROUPING_ID() AS gid
    FROM joined_subscriptions
    GROUP BY GROUPING SETS ((country, CASE WHEN credit_limit >= 17700 THEN 'PREMIUM' ELSE 'STANDARD' END), (country))
)
SELECT
    credit_limit_tier,
    new_york_total_credit_limit,
    row_count,
    city_list
FROM totals
ORDER BY credit_limit_tier DESC;
