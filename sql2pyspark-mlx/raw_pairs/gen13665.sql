WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance) OVER w_recent AS prev_balance,
        ROW_NUMBER() OVER w_recent AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance DESC, id) AS row_seq_2
    FROM accounts
    WHERE balance BETWEEN 17000 AND 53000
    WINDOW w_recent AS (PARTITION BY customer_id, account_type ORDER BY balance, id)
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, c.country
    FROM with_windows a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (2, 4, 28, 33)
        )
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, c.email
    FROM kept src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.opened_date,
        s.prev_balance,
        s.row_seq,
        top1.merchant,
        top1.transaction_amount
    FROM joined s
    JOIN LATERAL (
            SELECT c.merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM latest_match
    INTERSECT ALL
    SELECT id, customer_id
    FROM accounts
    WHERE account_type NOT IN ('SAVINGS', 'LOAN', 'CREDIT')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.credit_limit,
        c.country,
        c.segment
    FROM kept_rows s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
SELECT
    id,
    customer_id,
    opened_date,
    prev_balance,
    row_seq,
    merchant,
    credit_limit,
    CASE WHEN merchant RLIKE '^(7-Eleven|Lazada)$' THEN 'GROUP_B' WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS merchant_zone
FROM enriched_accounts
