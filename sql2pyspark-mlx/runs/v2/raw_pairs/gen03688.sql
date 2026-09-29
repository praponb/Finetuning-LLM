select *, trim(store_name) as clean_store_name from stores where upper(trim(store_name)) = 'DOWNTOWN EXPRESS' order by sqft desc, id limit 1
