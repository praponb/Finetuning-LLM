WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE status IN ('OPEN', 'CLOSED')
),
customer_id_summary AS (
    SELECT customer_id, MAX(balance) max_balance
    FROM valid_accounts
    GROUP BY customer_id
)
SELECT *
FROM customer_id_summary
WHERE max_balance >= 27000;
