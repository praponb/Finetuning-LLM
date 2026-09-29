SELECT id, sale_date, units, product_id FROM store_sales WHERE units <= 12 AND (units BETWEEN 20 AND 35 OR revenue <= 3700) ORDER BY id DESC, sale_date;
