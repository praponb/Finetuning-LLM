WITH valid AS (
    SELECT *, DATE_ADD(opened_date, 42) opened_date_plus_42d
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND customer_id IN (2, 5, 15, 28)
),
enriched AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) balance_dense_rank,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status,
        AVG(balance) OVER w_ordered balance_avg_2
    FROM valid
    WHERE opened_date_plus_42d > '2024-08-14'
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('WEEK', opened_date), account_type ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT
        src.*,
        d.name,
        d.age
    FROM enriched src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.balance_dense_rank,
        s.last_known_status,
        s.balance_avg_2,
        s.name,
        s.age,
        x.amount
    FROM joined s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged = FALSE
        )
),
kept_keys AS (
    SELECT customer_id
    FROM eligible
    EXCEPT DISTINCT
    SELECT customer_id
    FROM accounts
    WHERE balance < 40000
),
kept_rows AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
)
SELECT
    s.id,
    s.customer_id,
    s.balance_dense_rank,
    s.last_known_status,
    s.balance_avg_2,
    s.name,
    s.amount,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM kept_rows s
ORDER BY id DESC, balance_dense_rank
