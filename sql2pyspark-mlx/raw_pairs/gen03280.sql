WITH ranked_accounts AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS row_num
    FROM accounts
)
SELECT id, customer_id, balance
FROM ranked_accounts
WHERE row_num = 1;
