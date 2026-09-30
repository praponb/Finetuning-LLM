WITH enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(opened_date), account_type ORDER BY balance DESC NULLS FIRST) AS balance_dense_rank,
        FIRST_VALUE(status) IGNORE NULLS OVER (ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        LAG(balance, 2) OVER (PARTITION BY customer_id ORDER BY balance ASC, id ASC) AS prev_balance,
        LAG(balance, 1) OVER (PARTITION BY status, account_type ORDER BY opened_date ASC, id ASC) AS prev_balance_2
    FROM accounts
)
SELECT
    CASE WHEN prev_balance >= 86000 THEN 'LARGE' ELSE 'SMALL' END AS prev_balance_tier,
    APPROX_PERCENTILE(prev_balance_2, ARRAY(0.25, 0.5, 0.75)) AS prev_balance_2_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SIZE(COLLECT_SET(status)) AS num_status
FROM enriched
GROUP BY prev_balance_tier
HAVING num_status >= 1
ORDER BY prev_balance_tier DESC NULLS LAST;
