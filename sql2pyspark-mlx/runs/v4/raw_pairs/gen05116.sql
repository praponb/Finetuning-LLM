SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    AVG(balance) OVER (PARTITION BY customer_id, status ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) balance_avg_6,
    AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) balance_avg_3,
    ROW_NUMBER() OVER w_hist row_seq
FROM accounts
WINDOW w_hist AS (PARTITION BY customer_id ORDER BY balance, id);
