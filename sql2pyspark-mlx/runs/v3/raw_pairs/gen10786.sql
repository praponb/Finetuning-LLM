WITH numbered AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rnk
    FROM accounts
)
SELECT
    id,
    customer_id,
    balance,
    opened_date
FROM numbered
WHERE rnk <= 2;
