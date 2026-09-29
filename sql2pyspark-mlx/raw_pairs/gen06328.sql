WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(amount, 0) AS amount_filled
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-06-14 04:00:00')
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        p.opened_date,
        p.status
    FROM filtered src
    INNER JOIN accounts p
        ON src.account_id = p.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        opened_date,
        CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END AS is_frozen
    FROM enriched
    WHERE status IS NOT DISTINCT FROM 'FROZEN'
      AND account_id IN (25, 38)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        b.opened_date AS account_opened_date,
        b.status,
        b.balance
    FROM cleaned e
    LEFT JOIN accounts b
        ON e.account_id = b.id
)
SELECT
    id,
    txn_type,
    opened_date,
    is_frozen,
    account_opened_date,
    status,
    balance
FROM with_dims
ORDER BY balance DESC;
