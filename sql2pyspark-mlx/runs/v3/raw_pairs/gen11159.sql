SELECT id, sale_date, units, GREATEST(units, revenue) AS greatest_units_revenue, DATE_ADD(sale_date, 74) AS sale_date_plus_74d FROM store_sales
