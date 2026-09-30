SELECT id, units, sale_date, revenue, COALESCE(revenue, 0) revenue_filled, DATE_TRUNC('YEAR', sale_date) sale_date_period FROM store_sales WHERE NOT (units < 36 OR MONTH(sale_date) IN (1, 2, 9))
