SELECT id, units, product_id FROM store_sales WHERE units > (SELECT AVG(units) FROM store_sales WHERE YEAR(sale_date) = 2023) ORDER BY 3 DESC;
