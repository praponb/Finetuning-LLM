SELECT *, ROUND(revenue / units, 2) revenue_per_unit FROM store_sales ORDER BY units DESC, id LIMIT 3
