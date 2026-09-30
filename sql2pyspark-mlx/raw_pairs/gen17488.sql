SELECT
    *,
    LAG(balance) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id) AS prev_balance,
    ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY opened_date, id) AS seq_no
FROM accounts
WHERE opened_date BETWEEN '2023-08-01' AND '2025-03-01';
