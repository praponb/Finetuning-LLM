SELECT store_id, SUM(units) AS total_units, SUM(SUM(units)) OVER (ORDER BY store_id) AS cumulative_units FROM store_sales WHERE revenue BETWEEN 1100 AND 1400 GROUP BY store_id ORDER BY total_units;
