SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50,
    COUNT(DISTINCT method) AS distinct_methods,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles
FROM payments
GROUP BY year_start, quarter_start
HAVING distinct_methods >= 1
ORDER BY quarter_start DESC;
