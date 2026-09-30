SELECT
    order_items.product_id,
    order_items.quantity,
    products.cost,
    products.brand,
    suppliers.rating,
    CONCAT_WS(',', suppliers.city, suppliers.country) AS city_country_key
FROM order_items
RIGHT JOIN products ON order_items.product_id = products.id
FULL OUTER JOIN suppliers ON products.supplier_id = suppliers.id
WHERE products.stock_qty BETWEEN 150 AND 167;
