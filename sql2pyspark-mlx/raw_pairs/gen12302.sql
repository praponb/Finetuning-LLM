select o.id, o.amount, t.max_amount
from orders o
join (select order_id, max(amount) max_amount
    from payments
    group by order_id) t on o.id = t.order_id
where t.max_amount >= 500
order by max_amount desc;
