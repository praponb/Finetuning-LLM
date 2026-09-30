with joined as (select e.*, o.amount as order_amount from payments e left join orders o on e.order_id = o.id) select id, method, amount, paid_at, status, order_amount from joined;
