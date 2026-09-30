SELECT *, ROUND(revenue / units, 2) revenue_per_unit, revenue - units revenue_per_unit_2 FROM store_sales WHERE units BETWEEN 18 AND 20 ORDER BY units DESC
