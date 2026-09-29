WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, v
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.k,
        e.v,
        d.city,
        d.signup_date
    FROM flattened e
    LEFT JOIN customers d
        ON e.customer_id = d.id
    WHERE d.signup_date < '2023-03-01'
),
joined_carts AS (
    -- Attach customers attributes
    SELECT e.*, p.signup_date AS customer_signup_date
    FROM joined e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
joined_carts_2 AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.credit_limit,
        p.signup_date AS customer_signup_date_2,
        p.email
    FROM joined_carts a
    INNER JOIN customers p
        ON a.customer_id <=> p.id
)
SELECT
    s.customer_id,
    s.k,
    s.v,
    s.signup_date,
    s.credit_limit,
    s.customer_signup_date_2,
    s.email,
    (SELECT MAX(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS max_account_balance
FROM joined_carts_2 s
ORDER BY max_account_balance DESC;
