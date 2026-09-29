WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        ADD_MONTHS(last_restock, 1) AS last_restock_plus_months,
        ROUND(quantity * 1.1, 2) AS quantity_scaled
    FROM inventory
    WHERE product_id IN (2, 6, 8, 9)
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    COUNT(*) AS rows_total,
    MAX(a.quantity_scaled) AS max_quantity_scaled,
    MAX(a.quantity) AS max_quantity
FROM valid a
INNER JOIN products p ON a.product_id = p.id
GROUP BY p.brand
ORDER BY brand ASC NULLS FIRST
