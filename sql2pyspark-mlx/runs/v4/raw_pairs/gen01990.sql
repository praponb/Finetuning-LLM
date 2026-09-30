SELECT units, product_id FROM store_sales WHERE product_id = 2 UNION SELECT units, product_id FROM store_sales WHERE revenue BETWEEN 1700 AND 3900 ORDER BY units
