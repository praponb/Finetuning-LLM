select id, region, opened_date, store_name, ceil(sqft) as sqft_whole from stores where store_name not like 'Ma%';
