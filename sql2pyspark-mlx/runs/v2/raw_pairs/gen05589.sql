WITH RankedCustomers AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY signup_date DESC) AS rn
    FROM customers
)
SELECT
    id,
    country,
    signup_date,
    name
FROM RankedCustomers
WHERE rn <= 3;
