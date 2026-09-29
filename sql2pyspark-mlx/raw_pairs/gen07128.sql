SELECT
    order_items.order_id,
    products.product_name,
    products.launch_date,
    suppliers.city
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
INNER JOIN suppliers ON suppliers.id = products.supplier_id
ORDER BY city DESC;
