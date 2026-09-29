WITH AccountTotals AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_amazon
    FROM transactions
    GROUP BY account_id
)
SELECT a.balance, a.customer_id, A.pct_amazon
FROM accounts a
LEFT JOIN AccountTotals A ON a.id = A.account_id
ORDER BY customer_id, pct_amazon
