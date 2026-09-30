select opened_date, sqft, coalesce(sqft, 0) as sqft_filled, trim(store_name) as clean_store_name from stores where sqft <= 15114 or sqft < 5272
