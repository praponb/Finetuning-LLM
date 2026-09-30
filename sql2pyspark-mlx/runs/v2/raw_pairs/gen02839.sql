SELECT * FROM (SELECT store_id, ROUND(AVG(units), 2) AS avg_units FROM store_sales GROUP BY store_id) AS x WHERE avg_units >= 67;
