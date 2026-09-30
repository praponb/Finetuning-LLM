WITH ProductSummary AS (
    SELECT product_id, MAX(last_restock) last_last_restock, ROUND(AVG(quantity), 2) avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.cost, P.last_last_restock, P.avg_quantity
FROM products p
LEFT OUTER JOIN ProductSummary P ON p.id = P.product_id
WHERE P.avg_quantity >= 61
ORDER BY avg_quantity DESC;
