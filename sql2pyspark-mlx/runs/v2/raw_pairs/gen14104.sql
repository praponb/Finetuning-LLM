SELECT ss.id, ss.units, ss.store_id
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE sqft BETWEEN 8421 AND 14603)
ORDER BY units;
