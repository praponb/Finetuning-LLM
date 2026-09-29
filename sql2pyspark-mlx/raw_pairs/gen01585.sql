WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', plan, status) AS plan_status_key
    FROM subscriptions
    WHERE start_date >= '2023-03-01'
      AND customer_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.start_date,
        src.end_date,
        src.monthly_fee,
        c.city,
        c.credit_limit
    FROM valid src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(credit_limit) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC) AS next_credit_limit,
        LAG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC) AS prev_monthly_fee,
        LEAD(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC) AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC) AS seq_no
    FROM with_dims
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.start_date,
        e.next_credit_limit,
        e.next_monthly_fee,
        e.seq_no,
        b.city AS customer_city,
        b.signup_date,
        b.age
    FROM metrics e
    INNER JOIN customers b
        ON e.customer_id = b.id
    WHERE b.city IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.next_monthly_fee,
        src.age,
        b.signup_date AS customer_signup_date,
        b.age AS customer_age,
        b.email
    FROM joined src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
metrics_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(next_monthly_fee) OVER w_recent AS running_avg_next_monthly_fee,
        COUNT(next_monthly_fee) OVER w_recent AS running_count_next_monthly_fee,
        MIN(next_monthly_fee) OVER w_recent AS running_min_next_monthly_fee,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY next_monthly_fee DESC NULLS FIRST) AS next_monthly_fee_dense_rank
    FROM enriched
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY customer_signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined_subscriptions AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.next_monthly_fee,
        s.age,
        s.customer_signup_date,
        s.running_count_next_monthly_fee,
        s.running_min_next_monthly_fee,
        s.next_monthly_fee_dense_rank,
        c.signup_date
    FROM metrics_subscriptions s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    customer_signup_date,
    signup_date,
    MIN(running_count_next_monthly_fee) OVER w_grp AS last7_min_running_count_next_monthly_fee,
    MAX(running_count_next_monthly_fee) OVER w_grp AS last7_max_running_count_next_monthly_fee,
    AVG(running_count_next_monthly_fee) OVER w_grp AS last7_avg_running_count_next_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY next_monthly_fee ASC, id ASC) AS row_seq
FROM joined_subscriptions
WINDOW w_grp AS (PARTITION BY customer_id ORDER BY signup_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
