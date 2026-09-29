SELECT amount FROM payments WHERE method = 'paypal' EXCEPT SELECT unit_price FROM order_items WHERE unit_price BETWEEN 110 AND 380
