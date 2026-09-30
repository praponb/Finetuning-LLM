WITH matched_keys AS (
    SELECT id, account_id
    FROM transactions
    EXCEPT DISTINCT
    SELECT id, account_id
    FROM transactions
    WHERE merchant NOT IN ('Grab', '7-Eleven')
),
reconciled AS (
    SELECT s.*
    FROM transactions s
    JOIN matched_keys k ON s.id = k.id AND s.account_id = k.account_id
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    CASE WHEN txn_type RLIKE '^(FEE|CREDIT)$' THEN 'PRIORITY' WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS txn_type_zone,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM reconciled s;
