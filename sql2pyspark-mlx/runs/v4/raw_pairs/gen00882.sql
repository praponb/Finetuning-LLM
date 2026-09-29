WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM accounts
    EXCEPT ALL
    SELECT id, status
    FROM accounts
    WHERE account_type IN ('SAVINGS', 'CREDIT', 'CHECKING')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, p.credit_limit
    FROM kept src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(account_type) AS account_type_lower,
        COALESCE(status, 'NONE') AS status_filled
    FROM with_dims
    WHERE opened_date > DATE '2024-01-25'
      AND status IS NOT NULL
),
summary AS (
    -- Multi-level aggregation
    SELECT
        customer_id,
        SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM cleaned
    GROUP BY 1
    HAVING frozen_balance > 0
)
SELECT
    customer_id,
    frozen_balance,
    status_list
FROM summary
ORDER BY customer_id DESC NULLS FIRST
LIMIT 10;
