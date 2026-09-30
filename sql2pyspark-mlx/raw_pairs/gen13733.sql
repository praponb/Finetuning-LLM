WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(account_type) AS account_type_upper,
        ROUND(balance, 2) AS balance_rounded
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'OPEN'
      AND opened_date >= DATE '2023-05-27'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_type, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT account_type, id
    FROM accounts
    WHERE account_type IN ('CHECKING', 'LOAN', 'CREDIT')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.account_type = k.account_type AND s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        account_type_upper,
        balance_rounded,
        SUBSTRING(account_type, 1, 2) AS account_type_prefix,
        LOWER(status) AS status_lower
    FROM kept
    WHERE status IN ('CLOSED', 'FROZEN', 'OPEN')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        account_type,
        status,
        balance_rounded,
        account_type_prefix,
        status_lower,
        MAX(balance_rounded) OVER (PARTITION BY status) AS status_max_balance_rounded,
        RANK() OVER (ORDER BY balance DESC) AS balance_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id, account_type ORDER BY balance NULLS LAST) AS balance_percent_rank
    FROM filtered
    WHERE account_type = 'SAVINGS'
)
-- Grouped metrics
SELECT
    status,
    COUNT_IF(status = 'CLOSED') AS closed_events,
    COUNT(*) FILTER (WHERE account_type = 'LOAN') AS loan_rows,
    ROUND(AVG(balance_rounded), 2) AS avg_balance_rounded,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY ROLLUP (status)
HAVING closed_events >= 1
ORDER BY loan_rows
