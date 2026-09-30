select
    date_trunc('MONTH', s.opened_date) opened_month,
    s.city,
    count(distinct p.amount) unique_methods,
    max(p.amount) max_amount
from payments p
join orders o on o.id = p.order_id
join stores s on o.store_id = s.id
group by date_trunc('MONTH', s.opened_date), s.city
order by max_amount desc, s.city
