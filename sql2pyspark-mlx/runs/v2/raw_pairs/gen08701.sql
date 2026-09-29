select id, city, concat_ws('-', store_name, city) store_name_city_key from stores where region <> 'Central' and region = 'Central' and sqft <= 14794;
