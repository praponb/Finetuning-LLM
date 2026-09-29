WITH exploded AS (
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
joined AS (
    SELECT
        src.id,
        src.customer_id,
        c.credit_limit,
        c.city,
        c.signup_date
    FROM exploded src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
with_dims AS (
    SELECT
        e.id,
        e.customer_id,
        e.credit_limit,
        e.city,
        e.signup_date,
        p.city AS customer_city,
        p.signup_date AS customer_signup_date,
        p.age
    FROM joined e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
enriched AS (
    SELECT *, ROUND(age / SUM(age) OVER (PARTITION BY DATE_TRUNC('week', signup_date), city), 4) AS age_share
    FROM with_dims
    WHERE customer_city NOT IN ('Tokyo', 'Bangkok')
),
scored AS (
    SELECT *, ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY MONTH(signup_date)), 4) AS credit_limit_share
    FROM enriched
)
SELECT
    s.id,
    s.customer_id,
    s.city,
    s.signup_date,
    s.customer_signup_date,
    s.age,
    s.credit_limit_share,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM scored s;
