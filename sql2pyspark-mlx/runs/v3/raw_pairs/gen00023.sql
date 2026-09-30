SELECT unit_price, 'order_item' AS source FROM order_items WHERE order_id IN (4, 8, 25, 28) UNION SELECT cost AS unit_price, 'shipment' AS source FROM shipments ORDER BY source DESC
