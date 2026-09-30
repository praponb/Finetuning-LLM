WITH numbered AS (
    SELECT
        id,
        segment,
        age,
        credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS rn
    FROM customers
)
SELECT *
FROM numbered
WHERE rn <= 5;
