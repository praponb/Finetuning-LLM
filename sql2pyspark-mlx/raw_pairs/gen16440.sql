WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC NULLS FIRST, id) AS rn
        FROM subscriptions
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.status,
        p.credit_limit,
        p.signup_date
    FROM latest e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.age,
        b.name,
        b.email
    FROM joined e
    INNER JOIN customers b
        ON e.customer_id = b.id
    WHERE b.is_active
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.status,
        e.credit_limit,
        e.signup_date,
        e.age,
        e.email,
        b.credit_limit AS customer_credit_limit,
        b.city,
        b.signup_date AS customer_signup_date
    FROM enriched e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.customer_signup_date >= sub.start_date
       AND e.customer_signup_date <= DATE_ADD(sub.end_date, 3)
)
SELECT
    s.id,
    s.customer_id,
    s.status,
    s.credit_limit,
    s.age,
    s.email,
    s.customer_credit_limit,
    s.city,
    s.customer_signup_date,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM with_window s
ORDER BY credit_limit DESC
