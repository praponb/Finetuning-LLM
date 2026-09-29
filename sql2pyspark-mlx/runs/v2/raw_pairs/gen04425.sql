select * from (select store_id, avg(units) as avg_units from store_sales where revenue < 1400 group by store_id) as x where avg_units >= 36
