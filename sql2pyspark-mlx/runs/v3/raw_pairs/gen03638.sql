WITH transactions_by_account AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values, MAX(amount) AS max_amount
    FROM transactions
    WHERE merchant != 'Grab'
    GROUP BY account_id
)
SELECT
    a.balance,
    a.status,
    tba.merchant_values,
    tba.max_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.max_amount >= 3100
ORDER BY max_amount DESC;
