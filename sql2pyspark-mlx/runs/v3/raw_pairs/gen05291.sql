SELECT id, unit_price, ROW_NUMBER() OVER (ORDER BY unit_price, id) unit_price_row FROM order_items;
