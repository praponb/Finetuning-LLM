WITH TopCustomers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS rnk
    FROM customers
)
SELECT
    id,
    city,
    signup_date,
    email
FROM TopCustomers
WHERE rnk = 1
ORDER BY email DESC, id DESC;
