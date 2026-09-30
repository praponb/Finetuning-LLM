SELECT
    id,
    account_type,
    balance,
    opened_date,
    status,
    AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_balance,
    MIN(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_balance,
    SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_balance
FROM accounts;
