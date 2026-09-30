WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance,
        opened_date,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) balance_avg_2_prior,
        RANK() OVER (ORDER BY balance NULLS FIRST) balance_rank,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status
    FROM accounts
    WHERE opened_date <= '2024-02-11'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN balance >= 66000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(balance_avg_2_prior, 0.95) AS balance_avg_2_prior_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM metrics
GROUP BY DATE_TRUNC('QUARTER', opened_date), CASE WHEN balance >= 66000 THEN 'PREMIUM' ELSE 'STANDARD' END;
