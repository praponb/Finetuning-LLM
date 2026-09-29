SELECT *
FROM (
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) pos
    FROM accounts
) t
WHERE pos <= 5
