WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        NTILE(10) OVER (PARTITION BY account_type ORDER BY balance DESC, id) balance_bucket,
        FIRST_VALUE(balance) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance,
        SUM(balance) OVER (PARTITION BY account_type, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        SUM(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance
    FROM accounts
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_balance >= 346573 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_balance_tier,
    CASE WHEN first_known_balance >= 86000 THEN 'HIGH' ELSE 'LOW' END AS first_known_balance_tier,
    APPROX_PERCENTILE(first_known_balance, ARRAY(0.25, 0.5, 0.75)) AS first_known_balance_quartiles,
    SUM(CASE WHEN account_type = 'CREDIT' THEN first_known_balance ELSE 0 END) AS credit_first_known_balance,
    COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END) AS checking_count,
    MAX(balance) AS max_balance
FROM enriched
GROUP BY CASE WHEN cumulative_balance >= 346573 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN first_known_balance >= 86000 THEN 'HIGH' ELSE 'LOW' END
