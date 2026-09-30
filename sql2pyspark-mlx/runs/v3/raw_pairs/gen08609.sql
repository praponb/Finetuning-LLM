SELECT city, opened_date, sqft, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date FROM stores WHERE store_name LIKE '%Tow%'
