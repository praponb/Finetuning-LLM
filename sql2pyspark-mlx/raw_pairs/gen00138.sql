WITH numbered AS (
    SELECT
        id,
        customer_id,
        opened_date,
        balance,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS rnk
    FROM accounts
)
SELECT
    id,
    customer_id,
    opened_date,
    balance
FROM numbered
WHERE rnk <= 5
