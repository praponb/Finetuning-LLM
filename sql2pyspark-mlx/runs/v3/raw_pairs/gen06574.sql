select quarter(s.opened_date) opened_quarter, max(p.paid_at) last_paid_at, max(p.amount) max_amount
from payments p
join orders o on p.order_id = o.id
join stores s on o.store_id = s.id
group by quarter(s.opened_date)
order by opened_quarter desc, max_amount desc
limit 3;
