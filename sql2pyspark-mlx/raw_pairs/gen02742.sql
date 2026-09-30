SELECT
    CASE WHEN balance >= 80000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(balance) FILTER (WHERE account_type = 'LOAN') AS loan_max_balance
FROM accounts
GROUP BY 1
