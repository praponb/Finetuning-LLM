WITH RankedCustomers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS pos
    FROM customers
    WHERE email IS NOT NULL
)
SELECT id, city, signup_date
FROM RankedCustomers
WHERE pos <= 5;
