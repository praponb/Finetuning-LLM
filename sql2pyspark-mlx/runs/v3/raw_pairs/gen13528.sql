select id, sale_date, units, lead(units, 1) over (order by sale_date, id) as next_units from store_sales
