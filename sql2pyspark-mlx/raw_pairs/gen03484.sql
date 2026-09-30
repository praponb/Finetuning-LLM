WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label
    FROM inventory
    WHERE store_id IN (1, 4, 5)
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS cnt,
    MIN(a.quantity) AS min_quantity
FROM filtered a
JOIN products p ON a.product_id = p.id
GROUP BY p.category
ORDER BY min_quantity DESC NULLS LAST;
