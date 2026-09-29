SELECT ss.units, s.sqft
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
ORDER BY sqft DESC, 1;
