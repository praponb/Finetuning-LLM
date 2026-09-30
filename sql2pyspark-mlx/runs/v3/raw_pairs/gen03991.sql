SELECT
    suppliers.id,
    COUNT(products.id) AS product_count,
    SUM(products.cost) AS total_cost,
    SUM(products.stock_qty) AS total_stock_qty
FROM suppliers
LEFT OUTER JOIN products ON suppliers.id = products.supplier_id
GROUP BY suppliers.id
ORDER BY id, total_cost
LIMIT 10
