SELECT
    CASE WHEN amount >= 3100 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    PERCENTILE_APPROX(amount, 0.5) AS amount_p50,
    SUM(amount) AS total_amount,
    GROUPING_ID() AS grouping_level
FROM transactions
GROUP BY CUBE (CASE WHEN amount >= 3100 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING SUM(amount) < 0
ORDER BY total_amount;
