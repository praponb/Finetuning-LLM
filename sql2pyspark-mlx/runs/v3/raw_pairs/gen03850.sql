SELECT id, sale_date, units, store_id FROM store_sales WHERE sale_date <= (SELECT MIN(sale_date) FROM store_sales WHERE revenue BETWEEN 2800 AND 3000)
