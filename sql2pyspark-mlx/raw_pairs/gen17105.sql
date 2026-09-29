WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.code,
        d.segment
    FROM exploded s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
    WHERE d.email LIKE '%@company.co'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM joined
    EXCEPT DISTINCT
    SELECT id, customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.country,
        p.city,
        p.segment AS customer_segment
    FROM verified s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
SELECT
    s.id,
    s.customer_id,
    s.code,
    s.segment,
    s.customer_segment,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM with_dims s
ORDER BY min_account_balance DESC, id DESC
