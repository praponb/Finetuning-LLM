WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        FIRST_VALUE(balance) IGNORE NULLS OVER w_grp AS first_known_balance,
        LAG(balance, 2, 0) OVER (PARTITION BY customer_id ORDER BY balance, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY opened_date, id) AS row_seq,
        MAX(balance) OVER (PARTITION BY status) AS status_max_balance
    FROM accounts
    WINDOW w_grp AS (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY first_known_balance DESC NULLS LAST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance NULLS FIRST, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < '2023-04-01'
        )
)
SELECT
    id,
    customer_id,
    status,
    first_known_balance,
    prev_balance,
    row_seq,
    status_max_balance,
    latest_rank
FROM eligible
ORDER BY prev_balance DESC, id;
