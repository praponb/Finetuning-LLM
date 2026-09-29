select o.id, o.order_date, o.status, t.amount_count from orders o left join (select order_id, count(amount) as amount_count from payments group by order_id) t on o.id = t.order_id
