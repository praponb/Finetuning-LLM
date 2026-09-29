select
    s.id,
    s.supplier_name,
    s.country,
    s.rating,
    latest.price
from suppliers s
left join lateral (
        select c.price
        from products c
        where c.supplier_id = s.id
        order by c.stock_qty desc, c.id desc
        limit 1
    ) latest on true;
