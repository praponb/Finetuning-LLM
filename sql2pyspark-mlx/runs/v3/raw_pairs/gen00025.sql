WITH transactions_by_account AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.opened_date,
    a.customer_id,
    tba.debit_amount,
    tba.merchant_list
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.debit_amount >= 66
ORDER BY customer_id DESC;
