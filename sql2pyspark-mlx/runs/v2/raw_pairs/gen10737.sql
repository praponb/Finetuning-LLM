SELECT customer_id, account_type, avg_balance
FROM (
    SELECT customer_id, account_type, ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    GROUP BY customer_id, account_type
) AS agg
WHERE avg_balance < 75;
