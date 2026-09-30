select id, city, sqft, store_name, first_value(store_name) over (partition by city order by sqft, id) as first_store_name from stores where opened_date <= '2023-06-01'
