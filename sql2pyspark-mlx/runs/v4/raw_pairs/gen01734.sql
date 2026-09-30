SELECT revenue FROM store_sales WHERE product_id IN (1, 4, 6, 8) EXCEPT SELECT shipping_fee AS revenue FROM orders ORDER BY revenue;
