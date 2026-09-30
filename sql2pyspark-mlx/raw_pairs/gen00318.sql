SELECT id, city, age
FROM (
    SELECT
        id,
        city,
        age,
        RANK() OVER (PARTITION BY city ORDER BY age) AS row_num
    FROM customers
    WHERE country = 'UK'
) AS t
WHERE row_num <= 1
ORDER BY id DESC
