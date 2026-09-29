select o.channel, concat_ws(', ', sort_array(collect_set(s.status))) as status_values, count(distinct s.cost) as unique_statuss
from shipments s
inner join orders o on s.order_id = o.id
group by o.channel
