WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        segment,
        signup_date,
        is_active,
        NTILE(3) OVER (ORDER BY credit_limit DESC, id ASC) AS credit_limit_bucket,
        LEAD(age, 1) OVER (PARTITION BY segment, city ORDER BY age ASC, id ASC) AS next_age,
        AVG(age) OVER (ORDER BY credit_limit ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS age_avg_2
    FROM customers
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        segment,
        signup_date,
        SUM(credit_limit_bucket) OVER (PARTITION BY segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit_bucket,
        LAG(age_avg_2, 1) OVER (PARTITION BY segment ORDER BY signup_date ASC, id ASC) AS prev_age_avg_2,
        LEAD(credit_limit_bucket, 1, 0) OVER (ORDER BY signup_date ASC, id ASC) AS next_credit_limit_bucket
    FROM scored
),
with_windows_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY prev_age_avg_2 ASC, id ASC) AS event_seq,
        LAG(cumulative_credit_limit_bucket, 1) OVER (PARTITION BY segment ORDER BY prev_age_avg_2 ASC, id ASC) AS prev_cumulative_credit_limit_bucket
    FROM with_windows
    WHERE prev_age_avg_2 <= 58
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC NULLS LAST, id ASC) = 1
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.segment,
        s.cumulative_credit_limit_bucket,
        s.next_credit_limit_bucket,
        s.prev_cumulative_credit_limit_bucket,
        latest.account_account_type,
        latest.account_status
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.account_type AS account_account_type, c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    cumulative_credit_limit_bucket,
    next_credit_limit_bucket,
    prev_cumulative_credit_limit_bucket,
    account_account_type
FROM latest_match
ORDER BY cumulative_credit_limit_bucket DESC NULLS LAST;
