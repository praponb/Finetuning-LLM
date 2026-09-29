SELECT id, order_id, unit_price, unit_price - LAG(unit_price, 2) OVER (PARTITION BY order_id ORDER BY unit_price, id) unit_price_change FROM order_items ORDER BY unit_price_change ASC
