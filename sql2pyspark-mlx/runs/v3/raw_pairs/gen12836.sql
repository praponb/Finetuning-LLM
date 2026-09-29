WITH ProductTotals AS (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE store_id = 1
    GROUP BY product_id
)
SELECT p.product_name, p.cost, P.total_quantity
FROM products p
JOIN ProductTotals P ON p.id = P.product_id
WHERE P.total_quantity >= 1130
ORDER BY product_name;
