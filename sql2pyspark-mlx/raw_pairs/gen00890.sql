WITH remaining_keys AS (
    SELECT account_type, status
    FROM accounts
    EXCEPT
    SELECT account_type, status
    FROM accounts
    WHERE customer_id IN (2, 16, 26, 40)
),
verified AS (
    SELECT s.*
    FROM accounts s
    INNER JOIN remaining_keys k ON s.account_type = k.account_type AND s.status = k.status
),
scored AS (
    SELECT
        id,
        opened_date,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY balance) AS balance_percent_rank,
        MAX(balance) OVER (PARTITION BY account_type, customer_id) AS account_type_max_balance
    FROM verified
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount < 1500
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY balance_percent_rank DESC NULLS LAST) AS balance_percent_rank_dense_rank,
        RANK() OVER (ORDER BY balance_percent_rank NULLS LAST) AS balance_percent_rank_rank
    FROM not_flagged
    WHERE account_type_max_balance = 71715
),
not_flagged_accounts AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND HOUR(c.txn_time) BETWEEN 10 AND 15
        )
),
enriched_accounts AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY balance_percent_rank_dense_rank) AS balance_percent_rank_dense_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY balance_percent_rank_dense_rank DESC) AS balance_percent_rank_dense_rank_dense_rank_2,
        RANK() OVER (ORDER BY balance_percent_rank DESC) AS balance_percent_rank_rank_2,
        PERCENT_RANK() OVER (ORDER BY balance_percent_rank) AS balance_percent_rank_percent_rank
    FROM not_flagged_accounts
)
SELECT
    id,
    opened_date,
    balance_percent_rank,
    account_type_max_balance,
    balance_percent_rank_dense_rank,
    balance_percent_rank_rank,
    balance_percent_rank_dense_rank_dense_rank,
    balance_percent_rank_dense_rank_dense_rank_2,
    balance_percent_rank_percent_rank
FROM enriched_accounts;
