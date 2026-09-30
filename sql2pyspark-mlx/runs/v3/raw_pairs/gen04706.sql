SELECT id, store_id, units, ROUND(units / SUM(units) OVER (PARTITION BY store_id), 3) AS units_share FROM store_sales WHERE units < 25 ORDER BY units
