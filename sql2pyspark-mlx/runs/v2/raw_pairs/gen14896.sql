SELECT
    order_items.id,
    order_items.product_id,
    order_items.unit_price,
    products.category,
    suppliers.supplier_name,
    suppliers.country
FROM order_items
INNER JOIN products ON products.id = order_items.product_id
INNER JOIN suppliers ON products.supplier_id = suppliers.id
WHERE suppliers.city = 'Berlin'
ORDER BY product_id DESC, id
LIMIT 1;
