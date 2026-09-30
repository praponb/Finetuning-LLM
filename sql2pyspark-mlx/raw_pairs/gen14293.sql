SELECT
    merchant,
    MONTH(txn_time) AS txn_month,
    SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY 1, 2
HAVING SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) > 41;
