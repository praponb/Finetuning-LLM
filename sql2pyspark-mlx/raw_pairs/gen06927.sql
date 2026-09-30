select method, concat_ws(', ', sort_array(collect_set(method))) as method_values
from payments
where not (hour(paid_at) between 10 and 16 and to_date(paid_at) < '2025-01-01')
group by method;
