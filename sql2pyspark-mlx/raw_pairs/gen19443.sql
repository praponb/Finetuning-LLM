SELECT id, store_id, LEAST(revenue, units) AS least_revenue_units
FROM store_sales
ORDER BY store_id, id
LIMIT 20
