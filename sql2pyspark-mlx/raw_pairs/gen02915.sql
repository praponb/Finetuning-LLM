WITH cleaned AS (
    SELECT
        *,
        LENGTH(status) AS status_length,
        CONCAT(account_type, ' ', status) AS account_type_status
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'FROZEN'
)
SELECT
    CASE WHEN balance >= 65000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    customer_id,
    COUNT(DISTINCT status) AS distinct_status,
    ROUND(AVG(balance), 2) AS avg_balance,
    STDDEV_SAMP(balance) AS balance_stddev,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS balance_per_closed,
    COUNT(DISTINCT status) AS distinct_status_2,
    PERCENTILE_APPROX(balance, 0.9) AS balance_p90
FROM cleaned
GROUP BY balance_tier, customer_id;
