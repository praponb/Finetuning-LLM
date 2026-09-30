SELECT
    inventory.id,
    inventory.quantity,
    inventory.product_id,
    products.supplier_id,
    suppliers.supplier_name,
    ROUND(products.cost * 1.15, 2) AS cost_adjusted
FROM inventory
INNER JOIN products ON products.id = inventory.product_id
LEFT JOIN suppliers ON products.supplier_id = suppliers.id;
