WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        COUNT(balance) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_balance,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_balance,
        NTILE(10) OVER (ORDER BY balance DESC, id ASC) AS balance_bucket
    FROM accounts
    WHERE opened_date BETWEEN '2024-08-01' AND '2025-05-01'
)
-- Grouped metrics
SELECT
    CASE WHEN running_count_balance >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_balance_tier,
    CASE WHEN running_avg_balance >= 89000 THEN 'HIGH' ELSE 'LOW' END AS running_avg_balance_tier,
    ROUND(AVG(running_avg_balance), 2) AS avg_running_avg_balance,
    ROUND(SUM(running_count_balance) / NULLIF(COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END), 0), 2) AS running_count_balance_per_savings,
    COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_rows
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN running_count_balance >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_avg_balance >= 89000 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN running_count_balance >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY avg_running_avg_balance DESC;
