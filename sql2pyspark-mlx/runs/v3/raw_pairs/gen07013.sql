WITH transactions_by_account AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.customer_id, tba.txn_type_values
FROM accounts a
LEFT OUTER JOIN transactions_by_account tba ON a.id = tba.account_id;
