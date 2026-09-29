select * from (select store_id, count(quantity) quantity_count from inventory group by store_id) sub where quantity_count < 8 order by quantity_count desc, store_id limit 20
