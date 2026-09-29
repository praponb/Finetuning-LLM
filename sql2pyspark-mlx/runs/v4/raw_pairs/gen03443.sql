WITH transactions_by_account AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.status, tba.merchant_list, tba.avg_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
