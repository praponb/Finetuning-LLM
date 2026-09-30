select store_id, min_units from (select store_id, min(units) as min_units from store_sales group by store_id) as x where min_units >= 29;
