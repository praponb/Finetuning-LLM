SELECT
    status,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT_IF(status = 'OPEN') AS open_events,
    PERCENTILE_APPROX(balance, 0.9) AS balance_p90
FROM accounts
GROUP BY 1
HAVING open_events < 1
ORDER BY open_events DESC
