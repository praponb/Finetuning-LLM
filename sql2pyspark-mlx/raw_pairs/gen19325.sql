SELECT store_id, revenue - units AS revenue_per_unit, revenue * units AS revenue_per_unit_2
FROM store_sales
WHERE store_id = 1;
