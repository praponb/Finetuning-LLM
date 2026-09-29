select store_id, min(units) as min_units from store_sales where sale_date <= '2024-02-01' group by 1 order by min_units desc, store_id limit 10;
