SELECT *
FROM (
    SELECT CASE WHEN amount >= 3100 THEN 'high' WHEN amount >= 900 THEN 'mid' ELSE 'low' END AS amount_bucket, AVG(amount) AS avg_amount, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount
    FROM transactions
    GROUP BY CASE WHEN amount >= 3100 THEN 'high' WHEN amount >= 900 THEN 'mid' ELSE 'low' END
) AS s
WHERE avg_amount < 2200
