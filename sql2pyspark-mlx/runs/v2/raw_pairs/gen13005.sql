SELECT sqft, COALESCE(store_name, 'unassigned') AS store_name_filled
FROM stores
ORDER BY store_name_filled;
