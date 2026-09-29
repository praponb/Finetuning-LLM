SELECT id, order_id, unit_price, NTILE(2) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) unit_price_ntile FROM order_items ORDER BY unit_price_ntile DESC;
