SELECT id, units, product_id FROM store_sales WHERE units > (SELECT AVG(units) FROM store_sales) ORDER BY units DESC, product_id DESC
