WITH Ranked AS (
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) AS row_num
    FROM customers
)
SELECT
    id,
    segment,
    signup_date,
    email
FROM Ranked
WHERE row_num <= 3
ORDER BY id DESC, email DESC;
