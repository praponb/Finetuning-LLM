SELECT opened_date FROM stores WHERE LOWER(TRIM(store_name)) = 'mall central' INTERSECT SELECT signup_date FROM customers
