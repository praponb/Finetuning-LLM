SELECT store_id, units, revenue * units revenue_per_unit
FROM store_sales
WHERE store_id IN (4, 6) OR units BETWEEN 23 AND 24;
