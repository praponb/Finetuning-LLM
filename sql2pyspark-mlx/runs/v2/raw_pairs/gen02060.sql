SELECT store_name, opened_date, region, LENGTH(region) AS region_length FROM stores WHERE sqft BETWEEN 4592 AND 14906 ORDER BY region DESC;
