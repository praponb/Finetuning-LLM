SELECT MONTH(s.opened_date) AS opened_month, s.region, COUNT(DISTINCT i.quantity) AS unique_stores FROM inventory i INNER JOIN stores s ON s.id = i.store_id GROUP BY MONTH(s.opened_date), s.region;
