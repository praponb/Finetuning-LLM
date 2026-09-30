SELECT *
FROM (
    SELECT
        id,
        customer_id,
        opened_date,
        balance,
        RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS pos
    FROM accounts
) AS t
WHERE pos <= 5;
