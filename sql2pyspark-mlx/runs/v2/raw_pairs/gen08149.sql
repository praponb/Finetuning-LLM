select o.status, concat_ws(', ', sort_array(collect_set(p.status))) as status_values, count(distinct p.amount) as unique_methods
from payments p
inner join orders o on o.id = p.order_id
inner join customers c on o.customer_id = c.id
group by o.status;
