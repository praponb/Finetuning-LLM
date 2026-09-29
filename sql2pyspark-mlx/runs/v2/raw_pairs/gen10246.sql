WITH top_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rn
    FROM accounts
)
SELECT id, customer_id, balance
FROM top_accounts
WHERE rn <= 3
ORDER BY 1 ASC, balance DESC
