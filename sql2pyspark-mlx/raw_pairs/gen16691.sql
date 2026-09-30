WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date >= s.signup_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        city,
        signup_date,
        credit_limit,
        age,
        AVG(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS age_avg_5_prior,
        CORR(credit_limit, age) OVER (PARTITION BY country) AS corr_credit_limit_age
    FROM eligible
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.balance
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        signup_date,
        credit_limit,
        age,
        age_avg_5_prior,
        corr_credit_limit_age,
        age_avg_5_prior * age AS age_avg_5_prior_times_age
    FROM latest_match
    WHERE city IS NOT NULL
),
report AS (
    SELECT
        id,
        name,
        signup_date,
        age
    FROM base
    ORDER BY id
    LIMIT 10
)
SELECT id, age
FROM report
