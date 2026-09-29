WITH TopAccounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS pos
    FROM accounts
)
SELECT id, customer_id, balance
FROM TopAccounts
WHERE pos <= 3
