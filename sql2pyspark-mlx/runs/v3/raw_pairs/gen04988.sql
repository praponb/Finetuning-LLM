select method, concat_ws(', ', sort_array(collect_set(method))) method_values
from payments
where status in ('SUCCESS', 'FAILED', 'REFUNDED') and order_id is not null
group by method
having count(case when status = 'FAILED' then 1 end) > 1
order by method_values, method
limit 3;
