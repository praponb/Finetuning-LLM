SELECT customer_id, avg_balance
FROM (
    SELECT customer_id, ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    GROUP BY customer_id
) AS x
WHERE avg_balance >= 80;
