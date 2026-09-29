SELECT store_id, units FROM store_sales WHERE store_id = 6 UNION ALL SELECT store_id, units FROM store_sales WHERE revenue <= 1700 ORDER BY units
