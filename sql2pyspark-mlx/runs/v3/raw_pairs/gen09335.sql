SELECT quantity, ROUND(quantity, 2) AS quantity_rounded FROM order_items WHERE product_id IS NOT NULL OR unit_price < 320
