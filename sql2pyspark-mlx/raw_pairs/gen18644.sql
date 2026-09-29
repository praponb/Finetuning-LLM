SELECT YEAR(opened_date) AS opened_year, region, COUNT(*) AS cnt FROM stores GROUP BY YEAR(opened_date), region HAVING COUNT(*) < 4;
