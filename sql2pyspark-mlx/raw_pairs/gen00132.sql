SELECT amount FROM orders WHERE amount IS NOT NULL EXCEPT SELECT unit_price FROM order_items WHERE unit_price IS NOT NULL;
