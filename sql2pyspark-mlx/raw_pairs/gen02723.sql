SELECT
    CASE WHEN balance >= 85000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    PERCENTILE_APPROX(balance, 0.95) AS balance_p95,
    COUNT(*) AS cnt,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count,
    APPROX_PERCENTILE(balance, 0.95) AS balance_p95_2
FROM accounts
GROUP BY CASE WHEN balance >= 85000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING PERCENTILE_APPROX(balance, 0.95) >= 0
ORDER BY row_count DESC NULLS FIRST;
