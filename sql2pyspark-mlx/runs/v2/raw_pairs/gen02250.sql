WITH ProductTotals AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    P.stddev_quantity,
    P.avg_quantity
FROM products p
LEFT JOIN ProductTotals P ON p.id = P.product_id
WHERE P.stddev_quantity > 61;
