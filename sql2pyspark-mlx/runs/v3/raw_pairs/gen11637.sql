select o.channel, count(*) as cnt, concat_ws(', ', sort_array(collect_set(p.method))) as method_values
from payments p
left join orders o on p.order_id = o.id
group by o.channel
order by o.channel desc
