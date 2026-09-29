SELECT customer_id, avg_balance
FROM (
    SELECT customer_id, ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    GROUP BY customer_id
) AS agg
WHERE avg_balance < 63
ORDER BY avg_balance DESC
