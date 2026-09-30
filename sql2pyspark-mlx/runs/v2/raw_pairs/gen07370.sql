select status, concat_ws(', ', sort_array(collect_set(method))) method_values
from payments
where amount >= 1140 and amount >= 1220 and amount > 950
group by status
order by method_values, status
limit 10
