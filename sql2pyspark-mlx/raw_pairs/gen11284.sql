WITH joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.credit_limit,
        c.age,
        c.city
    FROM accounts e
    INNER JOIN customers c
        ON e.customer_id = c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.country,
        c.name
    FROM joined src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        credit_limit,
        age,
        city,
        country,
        name,
        age - balance AS age_minus_balance,
        CASE WHEN age >= 73 THEN 'High' WHEN age >= 35 THEN 'Medium' ELSE 'Low' END AS age_level
    FROM enriched
    WHERE opened_date > DATE '2023-06-19'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        status,
        age,
        city,
        country,
        name,
        age_minus_balance,
        age_level,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY MONTH(opened_date), status ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(age_minus_balance, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age_minus_balance,
        LAST_VALUE(age) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age
    FROM filtered
),
joined_accounts AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.city AS customer_city,
        b.signup_date
    FROM with_windows src
    INNER JOIN customers b
        ON src.customer_id = b.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM joined_accounts src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.signup_date >= sub.start_date
       AND src.signup_date < sub.end_date + INTERVAL 30 DAYS
)
-- Window metrics over several different partitions
SELECT
    id,
    city,
    country,
    age_minus_balance,
    age_level,
    last_known_country,
    last_known_age_minus_balance,
    last_known_age,
    subscription_id,
    plan,
    monthly_fee,
    SUM(last_known_age_minus_balance) OVER (PARTITION BY country ORDER BY last_known_age_minus_balance, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_last_known_age_minus_balance,
    COUNT(last_known_age_minus_balance) OVER (PARTITION BY country ORDER BY last_known_age_minus_balance, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_last_known_age_minus_balance,
    MAX(last_known_age_minus_balance) OVER (PARTITION BY country ORDER BY last_known_age_minus_balance, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_last_known_age_minus_balance,
    SUM(subscription_id) OVER (PARTITION BY plan ORDER BY signup_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_subscription_id
FROM with_window
