WITH cleaned AS (
    SELECT
        *,
        CAST(balance AS INT) AS balance_int,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM accounts
    WHERE opened_date > DATE '2023-03-04'
      AND status IS DISTINCT FROM 'FROZEN'
)
SELECT
    id,
    balance,
    opened_date,
    balance_int,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS row_seq,
    LEAD(balance, 1) OVER (PARTITION BY account_type, customer_id ORDER BY balance, id) AS next_balance
FROM cleaned
WHERE status <> 'CLOSED'
