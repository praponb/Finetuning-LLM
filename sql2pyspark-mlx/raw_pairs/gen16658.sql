WITH filtered AS (
    SELECT
        *,
        CASE WHEN balance >= 82000 THEN 'High' WHEN balance >= 6000 THEN 'Medium' ELSE 'Low' END AS balance_tier,
        DATEDIFF(CURRENT_DATE(), opened_date) / 365.25 AS years_since_opened_date
    FROM accounts
    WHERE account_type = 'CREDIT'
      AND customer_id IS NOT NULL
)
SELECT
    CASE WHEN years_since_opened_date >= 61 THEN 'HIGH' ELSE 'LOW' END AS years_since_opened_date_tier,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    COUNT(*) FILTER (WHERE status = 'FROZEN') AS frozen_rows,
    STDDEV_SAMP(balance) AS balance_stddev,
    ROUND(AVG(balance), 2) AS avg_balance
FROM filtered
GROUP BY CASE WHEN years_since_opened_date >= 61 THEN 'HIGH' ELSE 'LOW' END
ORDER BY balance_stddev DESC NULLS LAST
