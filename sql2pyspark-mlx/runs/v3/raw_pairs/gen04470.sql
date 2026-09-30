SELECT YEAR(opened_date) AS opened_year, MIN(opened_date) AS first_opened_date FROM stores GROUP BY YEAR(opened_date) ORDER BY opened_year;
