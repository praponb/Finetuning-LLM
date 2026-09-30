select
    i.id,
    i.quantity,
    s.store_name,
    ceil(i.quantity) as quantity_whole
from inventory i
right join stores s on i.store_id = s.id
where i.store_id = 2 or s.sqft != 15481
order by quantity desc;
