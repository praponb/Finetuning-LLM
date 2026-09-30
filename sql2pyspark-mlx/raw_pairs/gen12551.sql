SELECT id, region, COUNT(*) OVER (PARTITION BY region) AS region_count
FROM stores
WHERE store_name LIKE 'Si%'
