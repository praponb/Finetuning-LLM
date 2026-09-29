WITH filtered AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM transactions
    WHERE txn_time >= DATE '2023-10-10'
      AND account_id IN (2, 12, 28)
)
SELECT
    src.id,
    src.account_id,
    src.txn_type,
    src.amount,
    src.txn_time,
    src.is_flagged,
    src.is_flagged_label,
    p.opened_date,
    p.account_type,
    p.balance
FROM filtered src
LEFT JOIN accounts p
    ON src.account_id = p.id;
