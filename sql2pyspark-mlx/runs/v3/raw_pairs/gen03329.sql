SELECT
    id,
    category,
    cost,
    supplier_id,
    ROUND(cost * 100, 2) cost_scaled
FROM products
WHERE category NOT IN ('Sports', 'Grocery', 'Books') AND cost < 420
