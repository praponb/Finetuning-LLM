select sum(sqft) total_sqft, min(opened_date) min_opened_date from stores where sqft between 12303 and 13007 or upper(trim(store_name)) = 'MALL CENTRAL' order by min_opened_date asc;
