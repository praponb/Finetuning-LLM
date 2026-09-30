select
    s.region,
    s.store_name,
    t.avg_quantity,
    t.quantity_count
from stores s
left join (select store_id, round(avg(quantity), 2) as avg_quantity, count(quantity) as quantity_count
    from inventory
    group by store_id) t on s.id = t.store_id
where t.quantity_count > 9;
