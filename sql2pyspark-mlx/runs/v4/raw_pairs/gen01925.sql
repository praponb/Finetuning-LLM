WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount ASC, id ASC) = 1
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.is_flagged,
    CASE WHEN txn_type RLIKE '^(CREDIT|FEE)$' THEN 'EXTENDED' WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone,
    (SELECT MAX(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS max_transaction_amount
FROM deduped s
ORDER BY amount ASC NULLS LAST, account_id DESC;
