SELECT unit_price FROM order_items WHERE product_id IS NOT NULL UNION ALL SELECT amount FROM transactions;
