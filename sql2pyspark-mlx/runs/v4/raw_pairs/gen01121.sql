SELECT
    *,
    AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS amount_avg_6,
    AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
FROM transactions
