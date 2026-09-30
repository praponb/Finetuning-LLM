WITH numbered AS (
    SELECT
        id,
        country,
        age,
        RANK() OVER (PARTITION BY country ORDER BY age DESC) AS row_num
    FROM customers
    WHERE is_active
)
SELECT id, country, age
FROM numbered
WHERE row_num = 1;
