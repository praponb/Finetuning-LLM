SELECT *
FROM (
    SELECT account_id, SUM(amount) AS total_amount
    FROM transactions
    GROUP BY account_id
) AS agg
WHERE total_amount > 11000
