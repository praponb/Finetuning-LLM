select o.status, sum(case when p.status = 'FAILED' then p.amount else 0 end) as failed_amount, count(p.amount) as amount_count
from payments p
left join orders o on o.id = p.order_id
group by o.status
