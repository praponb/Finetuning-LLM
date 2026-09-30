SELECT revenue, product_id, units, DATE_ADD(sale_date, 50) AS sale_date_plus_50d FROM store_sales WHERE units <> 25 OR product_id IN (1, 2, 9) ORDER BY units DESC;
