SELECT id, sale_date FROM store_sales WHERE sale_date <= (SELECT MIN(sale_date) FROM store_sales) ORDER BY sale_date DESC, id ASC LIMIT 10
