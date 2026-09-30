WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY age DESC) AS rnk
    FROM customers
    WHERE email LIKE '%@startup.io'
)
SELECT id, city, age
FROM Ranked
WHERE rnk <= 3
ORDER BY age DESC;
