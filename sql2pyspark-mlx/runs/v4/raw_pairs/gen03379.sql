SELECT
    id,
    customer_id,
    balance,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS row_num
FROM accounts;
