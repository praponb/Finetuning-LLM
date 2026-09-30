WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE opened_date > DATE '2023-04-13'
      AND customer_id IS NOT NULL
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_merchant
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        opened_date,
        balance_int,
        transaction_merchant
    FROM with_latest
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY balance_int NULLS LAST) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY transaction_merchant ORDER BY balance_int DESC) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
)
-- Grouped metrics
SELECT
    customer_id,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    SUM(balance_int) AS total_balance_int,
    APPROX_PERCENTILE(balance_int, 0.95) AS balance_int_p95,
    COUNT_IF(transaction_merchant = '7-Eleven') AS 7_eleven_events,
    COUNT_IF(transaction_merchant = 'Shell') AS shell_events
FROM kept
GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('YEAR', opened_date)), (customer_id), ());
