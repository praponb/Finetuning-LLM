select id, store_id, sale_date, units, sum(units) over (partition by store_id order by sale_date, id) as running_units from store_sales where store_id in (1, 5)
