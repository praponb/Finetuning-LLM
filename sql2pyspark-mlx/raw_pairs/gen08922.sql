WITH metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan, status ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) monthly_fee_avg_6,
        DENSE_RANK() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee NULLS LAST) monthly_fee_dense_rank
    FROM subscriptions
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.end_date,
    (SELECT MIN(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS min_account_balance
FROM metrics s;
