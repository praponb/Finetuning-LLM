SELECT id, units FROM store_sales WHERE units >= (SELECT MAX(units) FROM store_sales) ORDER BY units DESC, 1 DESC LIMIT 5
