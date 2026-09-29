WITH ranked_accounts AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS rnk
    FROM accounts
)
SELECT
    id,
    customer_id,
    opened_date,
    account_type
FROM ranked_accounts
WHERE rnk <= 3
ORDER BY customer_id DESC;
