SELECT launch_date FROM products WHERE stock_qty < 274 EXCEPT SELECT delivered_date AS launch_date FROM shipments WHERE order_id IS NULL;
