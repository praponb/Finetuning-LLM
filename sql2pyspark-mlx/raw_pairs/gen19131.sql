WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        CONCAT_WS('-', status, account_type) AS status_account_type_key,
        CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END AS is_closed,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-08-18'
      AND status IS NOT NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_merchant
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        status_account_type_key,
        is_closed,
        status_filled,
        transaction_merchant,
        LEAD(is_closed, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_is_closed,
        SUM(balance) OVER (PARTITION BY account_type, status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
    FROM with_last
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        status_account_type_key,
        status_filled,
        transaction_merchant,
        next_is_closed,
        LEAST(is_closed, next_is_closed) AS least_is_closed_next_is_closed,
        COALESCE(transaction_merchant, 'DIRECT') AS resolved_transaction_merchant
    FROM scored
    WHERE transaction_merchant IS NOT NULL
      AND transaction_merchant IS NOT DISTINCT FROM 'Grab'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.is_flagged = FALSE
        )
)
-- Aggregate with conditional metrics
SELECT
    account_type,
    CASE WHEN least_is_closed_next_is_closed >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_is_closed_next_is_closed_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(least_is_closed_next_is_closed, ARRAY(0.25, 0.5, 0.75)) AS least_is_closed_next_is_closed_quartiles,
    ROUND(AVG(next_is_closed), 2) AS avg_next_is_closed,
    DENSE_RANK() OVER (PARTITION BY account_type ORDER BY ROUND(AVG(next_is_closed), 2) DESC) AS avg_next_is_closed_dense_rank
FROM not_flagged
GROUP BY account_type, CASE WHEN least_is_closed_next_is_closed >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END;
