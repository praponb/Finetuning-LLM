WITH Numbered AS (
    SELECT
        id,
        category,
        price,
        RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rnk
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'new')
)
SELECT id, category, price
FROM Numbered
WHERE rnk <= 3
ORDER BY id DESC
