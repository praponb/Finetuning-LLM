SELECT s.id, s.product_id, s.units FROM store_sales s WHERE s.units = (SELECT MIN(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) ORDER BY s.id DESC, s.units DESC;
