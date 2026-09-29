WITH with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY account_type, customer_id ORDER BY balance ASC NULLS FIRST) AS balance_cume_dist,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS balance_cume_dist_2
    FROM accounts
)
SELECT
    account_type,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS balance_per_open
FROM with_windows
GROUP BY ROLLUP (account_type, DATE_TRUNC('MONTH', opened_date))
ORDER BY activity_month DESC
