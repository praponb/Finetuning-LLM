SELECT id, opened_date, region, SUBSTRING(store_name, 1, 3) AS store_name_prefix FROM stores WHERE opened_date < '2024-05-02' AND city <> 'New York'
