select o.status, max(p.amount) as max_amount, sort_array(collect_set(p.method)) as method_list
from payments p
inner join orders o on o.id = p.order_id
inner join stores s on o.store_id = s.id
group by o.status;
