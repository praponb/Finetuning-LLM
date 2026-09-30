WITH joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.balance,
        a.opened_date,
        p.email
    FROM accounts a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        SUM(balance) OVER (ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance_2,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY balance ASC, id ASC) balance_bucket,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) balance_avg_5,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY balance ASC, id ASC) balance_bucket_2
    FROM joined
    WHERE balance < 46000
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        opened_date,
        email,
        balance_bucket,
        SUM(balance_bucket) OVER (PARTITION BY customer_id ORDER BY balance_avg_5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance_bucket,
        ROW_NUMBER() OVER (ORDER BY running_balance_2 DESC, id ASC) event_seq,
        SUM(balance_avg_5) OVER (PARTITION BY customer_id) customer_id_sum_balance_avg_5
    FROM with_windows
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND TO_DATE(ch.txn_time) < '2025-05-01'
        )
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        COUNT(*) AS row_count,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(customer_id_sum_balance_avg_5), 2) AS avg_customer_id_sum_balance_avg_5
    FROM kept
    GROUP BY activity_month
    HAVING COUNT(*) > 1
)
SELECT
    activity_month,
    cnt,
    avg_customer_id_sum_balance_avg_5
FROM grouped;
