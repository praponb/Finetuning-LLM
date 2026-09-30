SELECT opened_date, region, city, DATE_TRUNC('YEAR', opened_date) AS opened_date_period FROM stores WHERE store_name LIKE 'Sia%' ORDER BY region
