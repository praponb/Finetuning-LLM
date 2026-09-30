SELECT quantity FROM order_items WHERE quantity BETWEEN 4 AND 6 EXCEPT SELECT quantity FROM order_items WHERE unit_price IS NOT NULL ORDER BY quantity
