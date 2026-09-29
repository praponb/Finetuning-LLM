WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, customer_id ORDER BY balance NULLS LAST) AS row_num
        FROM accounts
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS seq_no,
        SUM(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        MAX(balance) OVER (PARTITION BY customer_id) AS customer_id_max_balance
    FROM ranked
    WHERE balance <= 21000
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM scored e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.opened_date >= sub.start_date
       AND e.opened_date < sub.end_date + INTERVAL '14' DAY
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant IN ('7-Eleven', 'Amazon', 'Shell')
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        row_num,
        subscription_id,
        AVG(cumulative_balance) OVER w_grp AS running_avg_cumulative_balance,
        MAX(cumulative_balance) OVER w_grp AS running_max_cumulative_balance,
        MIN(cumulative_balance) OVER w_grp AS running_min_cumulative_balance
    FROM eligible
    WINDOW w_grp AS (PARTITION BY account_type, customer_id ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN running_max_cumulative_balance >= 382000 THEN 'HIGH' ELSE 'LOW' END AS running_max_cumulative_balance_tier,
    ROUND(AVG(running_min_cumulative_balance), 2) AS avg_running_min_cumulative_balance,
    APPROX_PERCENTILE(subscription_id, 0.95) AS subscription_id_p95,
    MIN(running_min_cumulative_balance) AS min_running_min_cumulative_balance
FROM enriched
GROUP BY CASE WHEN running_max_cumulative_balance >= 382000 THEN 'HIGH' ELSE 'LOW' END
HAVING APPROX_PERCENTILE(subscription_id, 0.95) < 1000000
ORDER BY avg_running_min_cumulative_balance NULLS LAST;
