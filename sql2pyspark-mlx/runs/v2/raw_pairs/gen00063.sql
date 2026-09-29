SELECT id, units FROM store_sales WHERE units <= (SELECT MIN(units) FROM store_sales WHERE revenue >= 2500) ORDER BY id DESC
