SELECT
    CASE WHEN balance >= 81000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    account_type,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM accounts
GROUP BY 1, 2
HAVING COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) > 1
ORDER BY balance_tier DESC NULLS LAST;
