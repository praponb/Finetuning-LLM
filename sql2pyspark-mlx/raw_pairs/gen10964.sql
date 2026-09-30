SELECT
    store_sales.units,
    products.stock_qty,
    products.product_name,
    suppliers.city,
    suppliers.rating
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
RIGHT JOIN suppliers ON suppliers.id = products.supplier_id
WHERE suppliers.city IN ('Chiang Mai', 'Berlin');
