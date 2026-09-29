SELECT *
FROM (
    SELECT
        merchant,
        YEAR(txn_time) AS txn_year,
        SUM(amount) AS total_amount,
        MAX(amount) AS max_amount
    FROM transactions
    GROUP BY merchant, YEAR(txn_time)
) AS x
WHERE total_amount > 16000
ORDER BY max_amount DESC;
