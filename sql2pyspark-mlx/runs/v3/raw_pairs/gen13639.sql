SELECT MAX(units) - MIN(units) AS units_range, COUNT(*) AS cnt FROM store_sales WHERE store_id IN (3, 4, 5, 6) AND (product_id IN (7, 8, 10) OR units <= 16) ORDER BY units_range DESC
