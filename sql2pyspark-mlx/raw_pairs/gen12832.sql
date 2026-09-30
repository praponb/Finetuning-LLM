WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.opened_date,
        a.status,
        p.signup_date
    FROM accounts a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.txn_type,
        latest.transaction_amount
    FROM with_dims s
    JOIN LATERAL (
            SELECT c.txn_type, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.signup_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        opened_date,
        transaction_amount,
        AVG(transaction_amount) OVER (PARTITION BY YEAR(signup_date), customer_id ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS transaction_amount_avg_3,
        SUM(transaction_amount) OVER w_hist AS cumulative_transaction_amount,
        SUM(transaction_amount) OVER w_hist AS running_transaction_amount,
        AVG(transaction_amount) OVER (PARTITION BY YEAR(signup_date) ORDER BY transaction_amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS transaction_amount_avg_5
    FROM latest_match
    WHERE transaction_amount <= 3400
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY transaction_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM accounts
    WHERE customer_id IN (5, 36)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
with_dims_accounts AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.opened_date,
        e.cumulative_transaction_amount,
        e.running_transaction_amount,
        e.transaction_amount_avg_5,
        d.segment
    FROM verified e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
)
SELECT
    customer_id,
    opened_date,
    cumulative_transaction_amount,
    transaction_amount_avg_5,
    segment,
    CASE WHEN segment RLIKE '^(Consumer|SMB)$' THEN 'GROUP_B' WHEN segment RLIKE '^(Corporate)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS segment_zone
FROM with_dims_accounts
ORDER BY cumulative_transaction_amount DESC NULLS LAST, segment;
