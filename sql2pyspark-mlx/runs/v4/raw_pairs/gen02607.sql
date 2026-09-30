SELECT *, LOWER(region) AS region_lower
FROM stores
WHERE LENGTH(store_name) < 10 OR MONTH(opened_date) IN (4, 7, 8);
