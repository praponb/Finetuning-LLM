WITH ranked_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS rnk
    FROM accounts
)
SELECT id, customer_id, opened_date
FROM ranked_accounts
WHERE rnk <= 1;
