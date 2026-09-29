select
    o.id,
    o.discount,
    o.amount,
    t.avg_amount,
    t.cash_count
from orders o
inner join (select order_id, avg(amount) as avg_amount, count(case when method = 'cash' then 1 end) as cash_count
    from payments
    group by order_id) t on o.id = t.order_id
where t.avg_amount >= 430
order by o.discount desc, t.cash_count desc
