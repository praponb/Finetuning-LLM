select
    i.id,
    i.store_id,
    i.product_id,
    s.opened_date,
    s.store_name,
    p.cost
from inventory i
left outer join stores s on s.id = i.store_id
inner join products p on p.id = i.product_id
where s.city = 'Bangkok' and p.product_name like 'L%';
