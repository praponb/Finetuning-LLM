SELECT
    CASE WHEN amount >= 4400 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    ROUND(AVG(amount), 2) AS avg_amount,
    RANK() OVER (PARTITION BY CASE WHEN amount >= 4400 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(amount), 2) DESC) AS avg_amount_rank
FROM transactions
GROUP BY CASE WHEN amount >= 4400 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', txn_time)
HAVING num_txn_types >= 1
