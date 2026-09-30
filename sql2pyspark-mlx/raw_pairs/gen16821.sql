WITH remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM subscriptions
    EXCEPT ALL
    SELECT status, id
    FROM subscriptions
    WHERE start_date >= '2025-04-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        CUME_DIST() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_cume_dist,
        PERCENT_RANK() OVER (ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_percent_rank,
        MIN(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_min_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan, customer_id) AS plan_avg_monthly_fee,
        CUME_DIST() OVER (ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_cume_dist_2
    FROM reconciled
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        plan,
        start_date,
        monthly_fee,
        monthly_fee_cume_dist,
        monthly_fee_percent_rank,
        customer_id_min_monthly_fee,
        plan_avg_monthly_fee,
        monthly_fee_cume_dist_2,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM scored
    WHERE monthly_fee >= 180
      AND start_date >= DATE '2023-09-07'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(plan) AS plan_length
    FROM valid
    WHERE monthly_fee_percent_rank BETWEEN 0.48 AND 0.51
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY monthly_fee_percent_rank DESC) AS monthly_fee_percent_rank_cume_dist,
        DENSE_RANK() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee_cume_dist_2 DESC NULLS LAST) AS monthly_fee_cume_dist_2_dense_rank
    FROM filtered
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.credit_limit,
        d.country,
        d.email
    FROM scored_subscriptions e
    JOIN customers d
        ON e.customer_id = d.id
)
SELECT
    s.customer_id,
    s.start_date,
    s.monthly_fee,
    s.customer_id_min_monthly_fee,
    s.plan_avg_monthly_fee,
    s.monthly_fee_cume_dist_2,
    s.plan_length,
    s.monthly_fee_percent_rank_cume_dist,
    s.monthly_fee_cume_dist_2_dense_rank,
    (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS avg_account_balance
FROM joined s
ORDER BY plan_avg_monthly_fee NULLS FIRST
