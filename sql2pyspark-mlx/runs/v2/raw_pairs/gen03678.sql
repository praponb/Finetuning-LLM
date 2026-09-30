SELECT store_name, region, city, DAY(opened_date) AS opened_date_day FROM stores WHERE region IN ('North', 'Central') AND UPPER(store_name) LIKE '%ALL%';
