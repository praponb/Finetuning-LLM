SELECT revenue, 'store_sale' AS source FROM store_sales WHERE MONTH(sale_date) IN (3, 4, 6) UNION SELECT amount, 'transaction' AS source FROM transactions
