SELECT city, INITCAP(TRIM(region)) AS region_title, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label FROM stores WHERE store_name LIKE 'Do%' AND city = 'Singapore' ORDER BY 2 DESC
