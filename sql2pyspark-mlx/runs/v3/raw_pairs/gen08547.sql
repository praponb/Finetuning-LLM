SELECT id, units, store_id, RANK() OVER (ORDER BY units DESC) AS rnk, ROUND(units / SUM(units) OVER (PARTITION BY store_id), 3) AS units_share FROM store_sales ORDER BY units_share DESC;
