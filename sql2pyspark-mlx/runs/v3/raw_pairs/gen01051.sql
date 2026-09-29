SELECT id, store_name, COALESCE(store_name, 'unassigned') AS store_name_filled, SUBSTRING(region, 1, 4) AS region_prefix FROM stores;
