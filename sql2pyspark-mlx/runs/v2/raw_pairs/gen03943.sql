SELECT
    order_items.id,
    order_items.unit_price,
    order_items.product_id,
    products.supplier_id,
    products.launch_date,
    suppliers.supplier_name,
    COALESCE(products.cost, 0) AS cost_filled
FROM order_items
JOIN products ON order_items.product_id = products.id
JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY order_items.id DESC;
