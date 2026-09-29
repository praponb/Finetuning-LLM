select id, region, sqft, store_name, floor(sqft) as sqft_whole from stores where lower(store_name) like '%all%';
