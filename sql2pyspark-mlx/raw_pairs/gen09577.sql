SELECT products.supplier_id, MAX(order_items.unit_price) - MIN(order_items.unit_price) unit_price_range
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
WHERE products.stock_qty = 362
GROUP BY products.supplier_id;
