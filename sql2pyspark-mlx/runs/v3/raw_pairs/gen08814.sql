select
    p.id,
    p.order_id,
    o.discount,
    o.amount,
    s.opened_date,
    round(p.amount, 0) as amount_rounded
from payments p
inner join orders o on p.order_id = o.id
left outer join stores s on o.store_id = s.id
where p.amount <= 630
order by amount_rounded;
