SELECT
    id,
    customer_id,
    opened_date,
    account_type,
    balance,
    LAG(opened_date, 2) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS prev_opened_date,
    PERCENT_RANK() OVER (PARTITION BY account_type, customer_id ORDER BY balance DESC) AS balance_pct_rank
FROM accounts;
