WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE txn_type IN ('CREDIT', 'DEBIT', 'FEE') AND amount >= 1200
),
agg AS (
    SELECT QUARTER(txn_time) AS txn_quarter, MIN(txn_time) AS first_txn_time
    FROM valid_transactions
    GROUP BY QUARTER(txn_time)
)
SELECT *
FROM agg;
