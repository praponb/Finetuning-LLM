SELECT amount AS value FROM payments WHERE method IS NOT NULL EXCEPT SELECT revenue AS value FROM store_sales;
