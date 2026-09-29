SELECT
    order_items.id,
    order_items.unit_price,
    orders.shipping_fee,
    orders.store_id,
    products.supplier_id
FROM order_items
LEFT OUTER JOIN orders ON order_items.order_id = orders.id
LEFT OUTER JOIN products ON products.id = order_items.product_id
WHERE ARRAY_CONTAINS(products.tags, 'eco') AND order_items.unit_price < 270
