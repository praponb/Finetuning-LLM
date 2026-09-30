SELECT store_id, AVG(revenue) AS avg_revenue, AVG(units) AS avg_units
FROM store_sales
GROUP BY store_id;
