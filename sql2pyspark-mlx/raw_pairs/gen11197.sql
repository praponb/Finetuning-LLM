select
    i.id,
    i.product_id,
    i.quantity,
    p.stock_qty,
    s.store_name,
    s.city
from inventory i
full outer join products p on i.product_id = p.id
inner join stores s on s.id = i.store_id
where i.product_id = 6 and s.city not in ('Tokyo', 'London')
