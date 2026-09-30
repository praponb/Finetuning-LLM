SELECT
    CASE WHEN amount >= 1390 THEN 'high' WHEN amount >= 340 THEN 'mid' ELSE 'low' END AS amount_bucket,
    status,
    MAX(amount) AS max_amount,
    SUM(CASE WHEN method = 'cash' THEN amount ELSE 0 END) AS cash_amount
FROM payments
GROUP BY 1, 2;
