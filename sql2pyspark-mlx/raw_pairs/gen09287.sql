WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM accounts
    EXCEPT
    SELECT id, customer_id
    FROM accounts
    WHERE account_type IN ('CREDIT', 'SAVINGS')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < '2025-07-01'
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM eligible
    EXCEPT DISTINCT
    SELECT id
    FROM accounts
    WHERE status NOT IN ('FROZEN', 'CLOSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.segment,
        p.email,
        p.age
    FROM kept_rows e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id, customer_id
    FROM accounts
    WHERE customer_id IS NULL
),
reconciled_accounts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled_accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
)
-- Join a dimension and aggregate
SELECT
    d.city,
    src.segment AS account_segment,
    COUNT(*) AS num_rows,
    MIN(src.balance) AS min_balance,
    GROUPING_ID() AS level_id
FROM kept src
LEFT JOIN customers d ON src.customer_id = d.id
GROUP BY ROLLUP (d.city, src.segment);
