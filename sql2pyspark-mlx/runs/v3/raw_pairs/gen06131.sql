SELECT YEAR(opened_date) AS opened_year, MIN(opened_date) AS first_opened_date FROM stores WHERE NOT (city != 'New York' AND MONTH(opened_date) IN (6, 7, 12)) GROUP BY YEAR(opened_date);
