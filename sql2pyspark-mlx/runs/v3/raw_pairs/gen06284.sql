SELECT product_id, quantity, CAST(quantity AS STRING) quantity_num FROM order_items WHERE unit_price <= 250
