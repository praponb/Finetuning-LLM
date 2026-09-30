WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        c.signup_date,
        c.name,
        c.email
    FROM exploded src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.name AS customer_name,
        p.email AS customer_email,
        p.age
    FROM enriched src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.signup_date,
        s.name,
        s.email,
        s.customer_name,
        s.customer_email,
        s.age,
        p.credit_limit,
        p.country
    FROM joined s
    LEFT JOIN customers p
        ON s.customer_id = p.id
    WHERE p.city IN ('Berlin', 'London')
)
SELECT
    s.id,
    s.customer_id,
    s.name,
    s.email,
    s.customer_name,
    s.customer_email,
    s.age,
    s.credit_limit,
    CASE WHEN customer_name RLIKE '^(Alice Wong|David Lee )$' THEN 'GROUP_B' WHEN customer_name RLIKE '^(bob smith|  Carol Diaz)$' THEN 'CORE' ELSE 'EXTENDED' END AS customer_name_zone,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM joined_carts s
ORDER BY id DESC NULLS LAST;
