select store_id, round(avg(quantity), 2) as avg_quantity from inventory group by store_id order by avg_quantity desc;
