WITH ranked AS (
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY amount DESC, id) = 1
)
SELECT
    s.id,
    s.account_id,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    (SELECT MIN(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS min_transaction_amount
FROM ranked s
ORDER BY account_id DESC NULLS FIRST, min_transaction_amount;
