SELECT
    id,
    account_type,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS seq_no,
    AVG(balance) OVER (PARTITION BY MONTH(opened_date) ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_avg_6
FROM accounts;
