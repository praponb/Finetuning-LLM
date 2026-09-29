WITH numbered AS (
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS pos
    FROM accounts
    WHERE customer_id IN (32, 33)
)
SELECT *
FROM numbered
WHERE pos <= 5;
