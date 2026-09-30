SELECT city, TRIM(region) AS clean_region FROM stores WHERE YEAR(opened_date) >= 2024 AND opened_date < '2024-02-03' ORDER BY city;
