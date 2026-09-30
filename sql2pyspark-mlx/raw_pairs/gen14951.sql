SELECT
    id,
    account_type,
    balance,
    opened_date,
    status,
    AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) balance_avg_6_prior,
    SUM(balance) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id ASC) seq_no,
    ROW_NUMBER() OVER (PARTITION BY MONTH(opened_date), customer_id ORDER BY opened_date ASC, id ASC) row_seq,
    AVG(balance) OVER (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) balance_avg_6_prior_2
FROM accounts
WHERE account_type = 'LOAN';
