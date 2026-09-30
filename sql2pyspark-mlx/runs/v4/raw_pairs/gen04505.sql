SELECT *, unit_price - quantity AS line_total FROM order_items WHERE quantity IS NOT NULL OR unit_price <= 310;
