SELECT
    suppliers.city,
    products.supplier_id,
    products.price,
    products.category,
    CAST(products.cost AS INT) cost_int
FROM suppliers
LEFT OUTER JOIN products ON suppliers.id = products.supplier_id
ORDER BY supplier_id DESC
