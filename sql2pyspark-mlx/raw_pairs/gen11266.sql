SELECT amount AS value FROM payments WHERE amount > 920 EXCEPT SELECT revenue AS value FROM store_sales WHERE MONTH(sale_date) IN (3, 6, 7);
