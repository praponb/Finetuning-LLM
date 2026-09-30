WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(country) AS country_length,
        TRIM(email) AS clean_email,
        COALESCE(city, 'NONE') AS city_clean
    FROM customers
    WHERE signup_date > DATE '2023-02-13'
      AND LENGTH(name) >= 14
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.signup_date
        )
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.city,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.clean_email,
        s.city_clean,
        latest.balance
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.signup_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(credit_limit) OVER w_part AS prev_credit_limit,
        LEAD(balance) OVER w_part AS next_balance,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM not_flagged_customers
    WHERE signup_date < '2023-09-01'
    WINDOW w_part AS (PARTITION BY city ORDER BY credit_limit, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(city, '/', email) AS city_email
    FROM scored
    WHERE next_balance BETWEEN 29000 AND 60000
)
SELECT
    id,
    name,
    email,
    city,
    signup_date,
    credit_limit,
    clean_email,
    city_clean,
    balance
FROM valid
ORDER BY id NULLS LAST;
