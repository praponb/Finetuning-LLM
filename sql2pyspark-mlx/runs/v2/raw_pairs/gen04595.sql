select max(sqft) max_sqft, max(opened_date) last_opened_date from stores where store_name like 'Ol%' and upper(trim(store_name)) = 'RIVERSIDE'
