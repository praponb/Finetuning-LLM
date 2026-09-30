SELECT
    id,
    customer_id,
    balance,
    LAG(balance, 1) OVER (ORDER BY opened_date ASC, id ASC) AS prev_balance,
    NTILE(4) OVER (PARTITION BY MONTH(opened_date), status ORDER BY balance ASC, id ASC) AS balance_bucket,
    SUM(balance) OVER (ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
    ROW_NUMBER() OVER w_running AS event_seq
FROM accounts
WHERE status != 'CLOSED'
WINDOW w_running AS (ORDER BY balance DESC, id ASC);
