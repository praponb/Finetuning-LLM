SELECT id, store_id, units, ROUND(units / SUM(units) OVER (PARTITION BY store_id), 3) units_share FROM store_sales WHERE units >= 14 ORDER BY units_share DESC
