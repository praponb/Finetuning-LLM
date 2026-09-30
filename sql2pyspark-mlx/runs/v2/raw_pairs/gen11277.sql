SELECT region, MAX(opened_date) AS last_opened_date FROM stores GROUP BY region ORDER BY region
