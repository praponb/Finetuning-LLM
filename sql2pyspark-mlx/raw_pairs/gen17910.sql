WITH enriched AS (
    SELECT
        id,
        txn_type,
        merchant,
        is_flagged,
        DENSE_RANK() OVER w_ordered AS amount_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS event_seq,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM transactions
    WHERE txn_time < '2023-10-01'
    WINDOW w_ordered AS (PARTITION BY YEAR(txn_time), account_id ORDER BY amount DESC NULLS FIRST)
)
SELECT
    id,
    txn_type,
    merchant,
    is_flagged,
    amount_dense_rank,
    event_seq,
    last_known_merchant,
    CASE WHEN txn_type RLIKE '^(REFUND|DEBIT)$' THEN 'CORE' ELSE 'EXTENDED' END AS txn_type_zone
FROM enriched
ORDER BY id NULLS FIRST;
