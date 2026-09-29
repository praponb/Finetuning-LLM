WITH not_flagged AS (
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount BETWEEN 2200 AND 2900
        )
),
filtered AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        ROUND(balance, 0) AS balance_rounded,
        ROUND(balance * 1.1, 2) AS adjusted_balance
    FROM not_flagged
    WHERE opened_date > DATE '2024-04-30'
      AND customer_id IS NOT NULL
),
latest_match AS (
    SELECT s.*, latest.merchant
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    SELECT
        src.*,
        c.country,
        c.credit_limit,
        c.email
    FROM latest_match src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
matched AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM joined s
    INNER JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date < sub.end_date + INTERVAL '7' DAY
)
SELECT *
FROM (
    SELECT
        status,
        country,
        balance_rounded
    FROM matched
) src
PIVOT (
    AVG(balance_rounded) AS value_sum,
    COUNT(1) AS n
    FOR country IN ('TH', 'SG')
);
