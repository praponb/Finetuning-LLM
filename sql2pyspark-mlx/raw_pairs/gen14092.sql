SELECT unit_price, order_id, ROUND(quantity, 0) quantity_rounded, ROUND(quantity * 1.15, 2) quantity_scaled FROM order_items WHERE unit_price <= 210 OR quantity < 5;
