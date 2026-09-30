SELECT id, store_id, units, CEIL(units) AS units_whole, ROUND(revenue, 2) AS revenue_rounded FROM store_sales ORDER BY id LIMIT 10
