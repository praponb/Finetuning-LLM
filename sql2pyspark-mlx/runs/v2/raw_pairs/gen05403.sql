SELECT MONTH(opened_date) AS opened_month, city, MIN(opened_date) AS min_opened_date FROM stores GROUP BY MONTH(opened_date), city;
