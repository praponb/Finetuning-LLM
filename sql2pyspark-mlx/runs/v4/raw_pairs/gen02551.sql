WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior,
        FIRST_VALUE(status) IGNORE NULLS OVER w_recent AS first_known_status,
        NTILE(10) OVER w_running AS balance_bucket,
        FIRST_VALUE(status) IGNORE NULLS OVER w_recent AS first_known_status_2
    FROM accounts
    WINDOW w_recent AS (PARTITION BY status ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY account_type ORDER BY balance DESC, id ASC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS balance_bucket_tier,
    SUM(CASE WHEN account_type = 'CREDIT' THEN balance_bucket ELSE 0 END) AS credit_balance_bucket,
    COUNT(*) FILTER (WHERE account_type = 'SAVINGS') AS savings_rows,
    COUNT_IF(account_type = 'CHECKING') AS checking_events,
    SIZE(COLLECT_SET(status)) AS num_status
FROM scored
GROUP BY CASE WHEN balance_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END;
