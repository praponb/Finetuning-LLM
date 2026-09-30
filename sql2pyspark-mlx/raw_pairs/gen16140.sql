WITH with_last AS (
    SELECT
        s.id,
        s.email,
        s.country,
        s.city,
        top1.account_balance,
        top1.status
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.balance AS account_balance, c.status
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    SELECT city
    FROM with_last
    INTERSECT ALL
    SELECT city
    FROM customers
    WHERE name NOT LIKE 'D%'
),
kept_rows AS (
    SELECT s.*
    FROM with_last s
    INNER JOIN remaining_keys k ON s.city = k.city
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.account_type NOT IN ('SAVINGS', 'CHECKING', 'LOAN')
        )
),
filtered AS (
    SELECT
        email,
        country,
        city,
        status,
        UPPER(status) AS status_upper
    FROM kept
    WHERE status IS NOT NULL
)
SELECT email, status_upper
FROM filtered;
